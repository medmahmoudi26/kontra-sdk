package revocation

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
)

func digestOf(sid string) string {
	sum := sha256.Sum256([]byte(sid))
	return hex.EncodeToString(sum[:])
}

// gate serves a revocation list; its answer can be changed while it runs.
type gate struct {
	status atomic.Int32
	body   atomic.Value // string
	hits   atomic.Int32
}

func newGate(t *testing.T, sessions ...string) (*gate, *httptest.Server) {
	t.Helper()
	g := &gate{}
	g.status.Store(http.StatusOK)
	g.revoke(sessions...)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.hits.Add(1)
		if r.URL.Path != Path || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(g.status.Load()))
		_, _ = w.Write([]byte(g.body.Load().(string)))
	}))
	t.Cleanup(srv.Close)
	return g, srv
}

func (g *gate) revoke(sessions ...string) {
	quoted := make([]string, len(sessions))
	for i, s := range sessions {
		quoted[i] = `"` + digestOf(s) + `"`
	}
	g.body.Store(`{"window_seconds": 1200, "sessions": [` + strings.Join(quoted, ",") + `]}`)
}

func TestDefaultURLIsTheGateOfTheJWKS(t *testing.T) {
	cases := map[string]string{
		"http://gate:8080/.well-known/jwks.json":       "http://gate:8080/v1/auth/revocations",
		"https://api.kontra.run/.well-known/jwks.json": "https://api.kontra.run/v1/auth/revocations",
		"http://127.0.0.1:9/jwks.json":                 "",
		"file:///etc/jwks.json":                        "",
		"http://gate:8080/.well-known/jwks.json?x=1":   "",
		"http://u:p@gate:8080/.well-known/jwks.json":   "",
		"": "",
	}
	for jwks, want := range cases {
		if got := DefaultURL([]string{jwks}); got != want {
			t.Errorf("DefaultURL(%q) = %q, want %q", jwks, got, want)
		}
	}
	if got := DefaultURL(nil); got != "" {
		t.Errorf("no JWKS: %q", got)
	}
	// Only the first JWKS names the gate.
	if got := DefaultURL([]string{"file:///k.json", "http://gate/.well-known/jwks.json"}); got != "" {
		t.Errorf("second JWKS used: %q", got)
	}
}

func TestValidURL(t *testing.T) {
	for _, ok := range []string{"http://gate:8080/v1/auth/revocations", "https://x.test/r"} {
		if err := ValidURL(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"gate:8080/x", "file:///x", "ftp://x/y", "http://", "https://u:p@x/y"} {
		if ValidURL(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestRefreshTakesTheGatesList(t *testing.T) {
	g, srv := newGate(t, "s1", "s2")
	l := New(srv.URL+Path, nil, log.NewNoopLogger())
	if l.Revoked("s1") || l.Loaded() {
		t.Fatal("a new list is empty and not loaded")
	}
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !l.Revoked("s1") || !l.Revoked("s2") || l.Revoked("s3") || !l.Loaded() {
		t.Fatal("wrong list")
	}
	// A new answer replaces the old one: entries past the gate's window drop off.
	g.revoke("s3")
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if l.Revoked("s1") || !l.Revoked("s3") {
		t.Fatal("the list was not replaced")
	}
}

func TestAFailedRefreshKeepsTheListInForce(t *testing.T) {
	g, srv := newGate(t, "s1")
	l := New(srv.URL+Path, nil, log.NewNoopLogger())
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	bad := map[string]func(){
		"500":            func() { g.status.Store(http.StatusInternalServerError) },
		"not JSON":       func() { g.body.Store("<html>") },
		"not a digest":   func() { g.body.Store(`{"window_seconds": 1, "sessions": ["s1"]}`) },
		"upper-case hex": func() { g.body.Store(`{"window_seconds": 1, "sessions": ["` + strings.ToUpper(digestOf("x")) + `"]}`) },
		"too large":      func() { g.body.Store(`{"sessions": [` + strings.Repeat(" ", maxBody) + `]}`) },
	}
	for name, breakIt := range bad {
		g.status.Store(http.StatusOK)
		g.revoke("s1")
		breakIt()
		if err := l.Refresh(context.Background()); err == nil {
			t.Errorf("%s: refresh succeeded", name)
		}
		if !l.Revoked("s1") {
			t.Errorf("%s: the list in force was dropped", name)
		}
	}
	srv.Close()
	if err := l.Refresh(context.Background()); err == nil || !l.Revoked("s1") {
		t.Errorf("unreachable gate: err=%v, still revoked=%v", err, l.Revoked("s1"))
	}
}

func TestAGateWithoutTheListMeansNoneIsRevoked(t *testing.T) {
	_, srv := newGate(t, "s1")
	l := New(srv.URL+"/elsewhere", nil, log.NewNoopLogger())
	if err := l.Refresh(context.Background()); !errors.Is(err, errNoList) {
		t.Fatalf("404: %v", err)
	}
	if l.Revoked("s1") || !l.Loaded() {
		t.Fatal("a 404 is an empty list")
	}
}

func TestRunRefreshesUntilCanceled(t *testing.T) {
	g, srv := newGate(t)
	l := New(srv.URL+Path, nil, log.NewNoopLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.Run(ctx, 20*time.Millisecond); close(done) }()
	g.revoke("late")
	deadline := time.Now().Add(5 * time.Second)
	for !l.Revoked("late") {
		if time.Now().After(deadline) {
			t.Fatal("a revocation never reached the list")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

// ---------------------------------------------------------------------- the claim mapper

var signingKey = func() *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return k
}()

type keys struct{}

func (keys) EcdsaKey(alg, kid string) (*ecdsa.PublicKey, error) {
	if alg != "ES256" || kid != "test" {
		return nil, errors.New("unknown key")
	}
	return &signingKey.PublicKey, nil
}
func (keys) HmacKey(string, string) ([]byte, error)        { return nil, errors.New("no HMAC") }
func (keys) RsaKey(string, string) (*rsa.PublicKey, error) { return nil, errors.New("no RSA") }
func (keys) SupportedMethods() []string                    { return []string{"ES256"} }
func (keys) Close()                                        {}

const audience = "kontra:temporal"

// bearer is a gate worker token: ES256, aud kontra:temporal, the party's worker permission.
func bearer(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	base := jwt.MapClaims{
		"sub": "acme", "permissions": []string{"acme:worker"}, "aud": audience,
		"exp": time.Now().Add(15 * time.Minute).Unix(),
	}
	for k, v := range claims {
		if v == nil {
			delete(base, k)
		} else {
			base[k] = v
		}
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, base)
	token.Header["kid"] = "test"
	signed, err := token.SignedString(signingKey)
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + signed
}

// mapper is what kontra-temporal's public frontend runs: Temporal's default JWT claim
// mapper, wrapped by Check.
func mapper(t *testing.T, sessions ...string) (authorization.ClaimMapper, *gate, *List) {
	t.Helper()
	g, srv := newGate(t, sessions...)
	l := New(srv.URL+Path, nil, log.NewNoopLogger())
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	inner := authorization.NewDefaultJWTClaimMapper(keys{}, &config.Authorization{}, log.NewNoopLogger())
	return Check(inner, l), g, l
}

func claimsOf(m authorization.ClaimMapper, token string) (*authorization.Claims, error) {
	return m.GetClaims(&authorization.AuthInfo{AuthToken: token, Audience: audience})
}

func denied(err error) bool {
	var pd *serviceerror.PermissionDenied
	return errors.As(err, &pd)
}

// TestARevokedSessionsWorkerTokenIsRefused is the review's F3: a key is revoked at the
// gate, and the worker token minted from its session -- valid by signature, expiry and
// audience, which is all the default mapper checks -- stops working at Temporal.
func TestARevokedSessionsWorkerTokenIsRefused(t *testing.T) {
	m, g, l := mapper(t)
	token := bearer(t, jwt.MapClaims{"sid": "key-session"})
	claims, err := claimsOf(m, token)
	if err != nil || claims.Namespaces["acme"] != authorization.RoleWorker {
		t.Fatalf("a live session's token: claims=%+v err=%v", claims, err)
	}
	g.revoke("key-session")
	if err := l.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := claimsOf(m, token); !denied(err) {
		t.Fatalf("a revoked session's token was accepted: %v", err)
	}
	// Another session of the same party is untouched.
	if _, err := claimsOf(m, bearer(t, jwt.MapClaims{"sid": "human-login"})); err != nil {
		t.Fatalf("another session refused: %v", err)
	}
}

func TestATokenWithNoSessionIsRefused(t *testing.T) {
	m, _, _ := mapper(t)
	for name, sid := range map[string]any{"missing": nil, "empty": "", "not a string": 7} {
		if _, err := claimsOf(m, bearer(t, jwt.MapClaims{"sid": sid})); !denied(err) {
			t.Errorf("sid %s: %v", name, err)
		}
	}
}

func TestWhatTheMapperRefusesStaysRefused(t *testing.T) {
	m, _, _ := mapper(t)
	other, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	forged := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": "acme", "sid": "s", "permissions": []string{"acme:worker"}, "aud": audience,
		"exp": time.Now().Add(time.Minute).Unix(),
	})
	forged.Header["kid"] = "test"
	signed, _ := forged.SignedString(other)
	cases := map[string]string{
		"forged":         "Bearer " + signed,
		"expired":        bearer(t, jwt.MapClaims{"sid": "s", "exp": time.Now().Add(-time.Minute).Unix()}),
		"wrong scheme":   strings.Replace(bearer(t, jwt.MapClaims{"sid": "s"}), "Bearer", "Basic", 1),
		"no separator":   strings.Replace(bearer(t, jwt.MapClaims{"sid": "s"}), "Bearer ", "Bearer", 1),
		"wrong audience": bearer(t, jwt.MapClaims{"sid": "s", "aud": "kontra:api"}),
	}
	for name, token := range cases {
		if _, err := claimsOf(m, token); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestACallWithoutATokenIsTheMappersBusiness(t *testing.T) {
	m, _, _ := mapper(t)
	claims, err := m.GetClaims(&authorization.AuthInfo{})
	if err != nil || claims == nil || claims.Subject != "" || len(claims.Namespaces) != 0 {
		t.Fatalf("no token: %+v %v", claims, err)
	}
}

func TestTheBearerSchemeIsCaseInsensitiveLikeTheMappers(t *testing.T) {
	m, _, _ := mapper(t, "gone")
	for _, scheme := range []string{"bearer", "BEARER", "Bearer"} {
		token := strings.Replace(bearer(t, jwt.MapClaims{"sid": "gone"}), "Bearer", scheme, 1)
		if _, err := claimsOf(m, token); !denied(err) {
			t.Errorf("%s: a revoked token passed: %v", scheme, err)
		}
	}
}

func ExampleDefaultURL() {
	fmt.Println(DefaultURL([]string{"http://gate:8080/.well-known/jwks.json"}))
	// Output: http://gate:8080/v1/auth/revocations
}
