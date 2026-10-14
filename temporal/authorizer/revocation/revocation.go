// Package revocation makes Temporal refuse the worker tokens of a session the gate has
// revoked, within seconds rather than when the token expires.
//
// Temporal's default JWT claim mapper checks a token's signature, expiry and audience and
// nothing else: it cannot know that the gate revoked the token's session (an API key
// revoked, a logout, a reused refresh token, an address taken off the allowlist). A
// worker token lives 15 minutes, so a stolen key's holder could otherwise keep polling the
// victim's task queues and starting workflows for that long after the owner revoked it.
//
// The gate publishes the sessions it revoked recently (GET /v1/auth/revocations: sha256
// digests of their ids, over a window longer than any worker token's life). A List polls
// it; Check wraps the default claim mapper so that, after the mapper has verified a token,
// the token's "sid" claim is looked up there and a revoked one is refused. Check only ever
// refuses more than the mapper: the mapper still decides what a token is worth.
//
// When the list cannot be fetched the last one fetched stays in force and the failure is
// logged: revocations happen at the gate, so a gate that cannot be reached is one that
// revokes nothing meanwhile, and stopping every worker of every party whenever the gate
// is down would turn a gate outage into a Temporal one. A token that names no session is
// refused: every token the gate mints for Temporal names one.
package revocation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// Path is where the gate publishes the list, next to its JWKS.
const Path = "/v1/auth/revocations"

// jwksPath is the gate's JWKS. A JWKS there means a gate, which publishes the list too.
const jwksPath = "/.well-known/jwks.json"

// maxBody bounds one answer: the gate lists at most 200,000 sessions (67 bytes each in
// JSON), far above what it revokes in a window.
const maxBody = 16 << 20

// failureLogEvery repeats the error of a list that keeps failing, every so many attempts.
const failureLogEvery = 60

var digest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// DefaultURL is the list of the gate that publishes jwks, the first of the server's JWKS
// URLs: for http(s)://host/.well-known/jwks.json, http(s)://host/v1/auth/revocations.
// Anything else (a file, another path) has no default, and "" is returned.
func DefaultURL(jwks []string) string {
	if len(jwks) == 0 {
		return ""
	}
	u, err := url.Parse(jwks[0])
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.Path != jwksPath || u.RawQuery != "" || u.Fragment != "" {
		return ""
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: Path}).String()
}

// ValidURL reports why raw cannot be a list's address, or nil.
func ValidURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return fmt.Errorf("%q is not an http(s) URL", raw)
	}
	return nil
}

// List is the gate's latest list of revoked sessions, refreshed by Run.
type List struct {
	url    string
	client *http.Client
	logger log.Logger

	mu       sync.RWMutex
	revoked  map[string]struct{} // sha256 hex of revoked session ids
	loaded   bool                // at least one answer was taken
	failures int                 // consecutive failed refreshes
}

// New returns an empty list of the gate at url. A nil client means one with a 10 s timeout.
func New(url string, client *http.Client, logger log.Logger) *List {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &List{url: url, client: client, logger: logger, revoked: map[string]struct{}{}}
}

// Revoked reports whether the session sid is on the list.
func (l *List) Revoked(sid string) bool {
	sum := sha256.Sum256([]byte(sid))
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, ok := l.revoked[hex.EncodeToString(sum[:])]
	return ok
}

// Loaded reports whether the list has taken at least one answer from the gate.
func (l *List) Loaded() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.loaded
}

type body struct {
	WindowSeconds int      `json:"window_seconds"`
	Sessions      []string `json:"sessions"`
}

// Refresh fetches the list once and replaces the one in force. On any error the one in
// force stays. A gate that answers 404 publishes no list (it predates it): the list is
// then empty, and that is logged.
func (l *List) Refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		l.replace(map[string]struct{}{})
		return errNoList
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("the gate answered %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(raw) > maxBody {
		return fmt.Errorf("the list is larger than %d bytes", maxBody)
	}
	var b body
	if err := json.Unmarshal(raw, &b); err != nil {
		return fmt.Errorf("the list is not JSON of the expected shape: %w", err)
	}
	next := make(map[string]struct{}, len(b.Sessions))
	for _, s := range b.Sessions {
		if !digest.MatchString(s) {
			return errors.New("the list holds an entry that is not a sha256 digest")
		}
		next[s] = struct{}{}
	}
	l.replace(next)
	return nil
}

// errNoList is a 404: the gate has no list to publish.
var errNoList = errors.New("the gate publishes no revocation list (404)")

func (l *List) replace(next map[string]struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.revoked = next
	l.loaded = true
}

// Run refreshes the list now and then every interval until ctx ends, logging when it
// starts failing, every failureLogEvery failures after that, and when it recovers.
func (l *List) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		attempt, cancel := context.WithTimeout(ctx, every+10*time.Second)
		err := l.Refresh(attempt)
		cancel()
		l.note(err)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *List) note(err error) {
	l.mu.Lock()
	failures := l.failures
	if err == nil {
		l.failures = 0
	} else {
		l.failures++
	}
	l.mu.Unlock()
	switch {
	case err == nil && failures > 0:
		l.logger.Info("revocation list: reachable again", tag.NewStringTag("url", l.url))
	case errors.Is(err, errNoList):
		if failures%failureLogEvery == 0 {
			l.logger.Warn("revocation list: the gate publishes none; a revoked session's worker "+
				"tokens work until they expire", tag.NewStringTag("url", l.url))
		}
	case err != nil && failures%failureLogEvery == 0:
		l.logger.Error("revocation list: cannot refresh it; the last one fetched stays in force",
			tag.NewStringTag("url", l.url), tag.Error(err))
	}
}

// Check wraps the claim mapper next: a token next accepts is refused if it names no
// session or a session on list. Tokens next refuses, and calls without a token, are
// answered exactly as next answers them.
func Check(next authorization.ClaimMapper, list *List) authorization.ClaimMapper {
	return checked{next: next, list: list}
}

type checked struct {
	next authorization.ClaimMapper
	list *List
}

func (c checked) GetClaims(info *authorization.AuthInfo) (*authorization.Claims, error) {
	claims, err := c.next.GetClaims(info)
	if err != nil || info == nil || info.AuthToken == "" {
		return claims, err
	}
	sid, err := session(info.AuthToken)
	if err != nil {
		return nil, err
	}
	if c.list.Revoked(sid) {
		return nil, serviceerror.NewPermissionDenied("the token's session was revoked", "")
	}
	return claims, nil
}

// session is the "sid" claim of a bearer token the wrapped mapper has just verified: the
// same string, split and decoded as Temporal's default mapper does (golang-jwt), so the
// claim read here is the one whose signature was checked.
func session(authToken string) (string, error) {
	scheme, raw, ok := strings.Cut(authToken, " ")
	if !ok || !strings.EqualFold(scheme, "bearer") {
		return "", serviceerror.NewPermissionDenied("unexpected authorization token format", "")
	}
	token, _, err := jwt.NewParser().ParseUnverified(raw, jwt.MapClaims{})
	if err != nil {
		return "", serviceerror.NewPermissionDenied("unexpected authorization token format", "")
	}
	claims, _ := token.Claims.(jwt.MapClaims)
	sid, _ := claims["sid"].(string)
	if sid == "" {
		return "", serviceerror.NewPermissionDenied("the token names no session", "")
	}
	return sid, nil
}
