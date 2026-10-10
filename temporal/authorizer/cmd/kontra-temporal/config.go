package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
)

// defaultAudience is the "aud" of the gate's Temporal worker tokens. The gate signs every
// kind of token with one key ring (API access, hold, browser state), and Temporal's claim
// mapper reads "permissions" from any token that key verifies unless an audience is
// configured, so the check is always on: an empty KONTRA_JWT_AUDIENCE means this default,
// never "no check".
const defaultAudience = "kontra:temporal"

// loopback is where every service that has no authorization of its own listens: history,
// matching and the system worker. They all run in this process, so nothing outside it
// ever needs to reach them, and there is deliberately no variable to change it.
const loopback = "127.0.0.1"

// service is one Temporal service in this process. Unless KONTRA_<env>_PORT is set, its
// gRPC port is KONTRA_PORT_BASE plus offset, which with the default base reproduces
// Temporal's usual ports (7233 frontend, 7236 internal frontend).
type service struct {
	name   primitives.ServiceName
	env    string
	offset int
}

var services = []service{
	{primitives.FrontendService, "FRONTEND", 0},
	{primitives.HistoryService, "HISTORY", 1},
	{primitives.MatchingService, "MATCHING", 2},
	{primitives.InternalFrontendService, "INTERNAL_FRONTEND", 3},
	{primitives.WorkerService, "WORKER", 6},
}

// settings is everything the server reads from its environment (README.md lists it).
type settings struct {
	rpc map[primitives.ServiceName]config.RPC

	db         string // "sqlite" or "postgres"
	sqliteFile string // empty: in memory
	pg         map[string]*config.SQL
	autoSetup  bool
	adminDB    string
	shards     int

	jwks        []string
	jwksRefresh time.Duration
	audience    string

	dynamicConfig     string
	dynamicConfigPoll time.Duration
	logLevel          string
	logFormat         string
	metricsAddr       string
}

func loadSettings() (*settings, error) {
	e := &env{}
	s := &settings{
		db:                e.str("KONTRA_DB", "sqlite"),
		sqliteFile:        e.str("KONTRA_SQLITE_FILE", ""),
		autoSetup:         e.flag("KONTRA_AUTO_SETUP"),
		adminDB:           e.str("KONTRA_DB_ADMIN_NAME", "postgres"),
		jwks:              e.list("KONTRA_JWKS_URLS"),
		jwksRefresh:       e.duration("KONTRA_JWKS_REFRESH", time.Minute),
		audience:          e.str("KONTRA_JWT_AUDIENCE", defaultAudience),
		dynamicConfig:     e.str("KONTRA_TEMPORAL_DYNAMIC_CONFIG", ""),
		dynamicConfigPoll: e.duration("KONTRA_DYNAMIC_CONFIG_POLL", 10*time.Second),
		logLevel:          e.str("KONTRA_LOG_LEVEL", "info"),
		logFormat:         e.str("KONTRA_LOG_FORMAT", "json"),
		metricsAddr:       e.str("KONTRA_METRICS_ADDR", ""),
	}

	base := e.integer("KONTRA_PORT_BASE", 7233)
	s.rpc = make(map[primitives.ServiceName]config.RPC, len(services))
	for _, svc := range services {
		s.rpc[svc.name] = config.RPC{
			BindOnIP: loopback,
			GRPCPort: e.integer("KONTRA_"+svc.env+"_PORT", base+svc.offset),
		}
	}
	// The public frontend: the only listener meant to be reachable from outside, behind
	// Kontra's TLS proxy (PRD §10.2). Its HTTP API is off unless asked for: the SDKs speak
	// gRPC, and every HTTP route there (HTTP API, Nexus HTTP API, Nexus callbacks) is denied
	// to tokens anyway.
	public := s.rpc[primitives.FrontendService]
	public.BindOnIP = e.str("KONTRA_FRONTEND_BIND_IP", loopback)
	public.HTTPPort = e.integer("KONTRA_FRONTEND_HTTP_PORT", 0)
	s.rpc[primitives.FrontendService] = public
	// The internal frontend: no authentication, full control. It must serve HTTP because
	// history delivers Nexus tasks and completions to it, and the gate relays quotes through
	// it (decisions I4). Loopback unless the deployment puts the gate on a private network
	// with it (compose); never publish its ports.
	internal := s.rpc[primitives.InternalFrontendService]
	internal.BindOnIP = e.str("KONTRA_INTERNAL_FRONTEND_BIND_IP", loopback)
	internal.HTTPPort = e.integer("KONTRA_INTERNAL_FRONTEND_HTTP_PORT", base+13)
	if internal.HTTPPort == 0 {
		e.fail("KONTRA_INTERNAL_FRONTEND_HTTP_PORT must not be 0")
	}
	s.rpc[primitives.InternalFrontendService] = internal

	switch s.db {
	case "sqlite":
		s.shards = e.integer("KONTRA_HISTORY_SHARDS", 1)
	case "postgres":
		s.shards = e.integer("KONTRA_HISTORY_SHARDS", 4)
		s.pg = postgres(e)
	default:
		e.fail(fmt.Sprintf("KONTRA_DB must be sqlite or postgres, not %q", s.db))
	}
	if s.shards < 1 {
		e.fail("KONTRA_HISTORY_SHARDS must be at least 1")
	}
	return s, e.err()
}

func postgres(e *env) map[string]*config.SQL {
	host := e.str("KONTRA_DB_HOST", "127.0.0.1")
	sql := func(name string) *config.SQL {
		cfg := &config.SQL{
			PluginName:      e.str("KONTRA_DB_PLUGIN", "postgres12_pgx"),
			DatabaseName:    name,
			ConnectAddr:     net.JoinHostPort(host, e.str("KONTRA_DB_PORT", "5432")),
			ConnectProtocol: "tcp",
			User:            e.str("KONTRA_DB_USER", "temporal"),
			Password:        e.str("KONTRA_DB_PASSWORD", ""),
			MaxConns:        e.integer("KONTRA_DB_MAX_CONNS", 20),
			MaxIdleConns:    e.integer("KONTRA_DB_MAX_CONNS", 20),
			MaxConnLifetime: time.Hour,
		}
		if e.flag("KONTRA_DB_TLS") {
			cfg.TLS = &auth.TLS{
				Enabled:                true,
				CaFile:                 e.str("KONTRA_DB_TLS_CA", ""),
				EnableHostVerification: true,
				ServerName:             host,
			}
		}
		return cfg
	}
	return map[string]*config.SQL{
		"postgres":            sql(e.str("KONTRA_DB_NAME", "temporal")),
		"postgres-visibility": sql(e.str("KONTRA_DB_VISIBILITY_NAME", "temporal_visibility")),
	}
}

// hosts is the static membership of this single process: every service at its loopback
// gRPC address. No membership protocol runs and no membership port is opened.
func (s *settings) hosts() map[primitives.ServiceName]static.Hosts {
	hosts := make(map[primitives.ServiceName]static.Hosts, len(s.rpc))
	for name := range s.rpc {
		hosts[name] = static.SingleLocalHost(s.address(name, false))
	}
	return hosts
}

// temporalConfig builds the server configuration. There is no publicClient section: with
// an internal frontend configured, Temporal's own clients (the system worker, history's
// Nexus calls and completions) use the internal frontend.
func (s *settings) temporalConfig(dynamicConfigPath string) *config.Config {
	cfg := &config.Config{
		Global: config.Global{
			Authorization: config.Authorization{
				JWTKeyProvider:       config.JWTKeyProvider{KeySourceURIs: s.jwks, RefreshInterval: s.jwksRefresh},
				PermissionsClaimName: "permissions",
				Audience:             s.audience,
			},
		},
		Persistence: s.persistence(),
		Log:         log.Config{Stdout: true, Level: s.logLevel, Format: s.logFormat},
		ClusterMetadata: &cluster.Config{
			FailoverVersionIncrement: 10,
			MasterClusterName:        "active",
			CurrentClusterName:       "active",
			ClusterInformation: map[string]cluster.ClusterInformation{"active": {
				Enabled:                true,
				InitialFailoverVersion: 1,
				RPCAddress:             s.address(primitives.InternalFrontendService, false),
				HTTPAddress:            s.address(primitives.InternalFrontendService, true),
			}},
		},
		DCRedirectionPolicy: config.DCRedirectionPolicy{Policy: "noop"},
		Services:            make(map[string]config.Service, len(s.rpc)),
		Archival: config.Archival{
			History:    config.HistoryArchival{State: "disabled"},
			Visibility: config.VisibilityArchival{State: "disabled"},
		},
		NamespaceDefaults: config.NamespaceDefaults{Archival: config.ArchivalNamespaceDefaults{
			History:    config.HistoryArchivalNamespaceDefaults{State: "disabled"},
			Visibility: config.VisibilityArchivalNamespaceDefaults{State: "disabled"},
		}},
		DynamicConfigClient: &dynamicconfig.FileBasedClientConfig{
			Filepath:     dynamicConfigPath,
			PollInterval: s.dynamicConfigPoll,
		},
	}
	for name, rpc := range s.rpc {
		cfg.Services[string(name)] = config.Service{RPC: rpc}
	}
	if s.metricsAddr != "" {
		cfg.Global.Metrics = &metrics.Config{Prometheus: &metrics.PrometheusConfig{
			ListenAddress: s.metricsAddr, HandlerPath: "/metrics", TimerType: "histogram",
		}}
	}
	return cfg
}

func (s *settings) persistence() config.Persistence {
	if s.db == "postgres" {
		stores := make(map[string]config.DataStore, len(s.pg))
		for name, sql := range s.pg {
			stores[name] = config.DataStore{SQL: sql}
		}
		return config.Persistence{
			DefaultStore:     "postgres",
			VisibilityStore:  "postgres-visibility",
			NumHistoryShards: int32(s.shards),
			DataStores:       stores,
		}
	}
	sqlite := &config.SQL{PluginName: "sqlite", ConnectAddr: "localhost", ConnectProtocol: "tcp"}
	if s.sqliteFile == "" {
		// Connections with the same name share one in-memory database within the process.
		sqlite.DatabaseName = fmt.Sprintf("kontra-%d", rand.IntN(1<<30))
		sqlite.ConnectAttributes = map[string]string{"mode": "memory", "cache": "shared"}
	} else {
		sqlite.DatabaseName = s.sqliteFile
		sqlite.ConnectAttributes = map[string]string{
			"mode": "rwc", "setup": "true", "journal_mode": "wal", "busy_timeout": "10000",
		}
	}
	return config.Persistence{
		DefaultStore:     "sqlite",
		VisibilityStore:  "sqlite",
		NumHistoryShards: int32(s.shards),
		DataStores:       map[string]config.DataStore{"sqlite": {SQL: sqlite}},
	}
}

// address is where this process reaches one of its services: loopback and the service's
// gRPC (or HTTP) port.
func (s *settings) address(name primitives.ServiceName, http bool) string {
	port := s.rpc[name].GRPCPort
	if http {
		port = s.rpc[name].HTTPPort
	}
	return net.JoinHostPort(loopback, strconv.Itoa(port))
}

// env reads variables and collects every malformed one, so a bad configuration fails
// once with the full list.
type env struct{ errs []error }

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) integer(key string, def int) int {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		e.fail(fmt.Sprintf("%s=%q is not a non-negative integer", key, v))
	}
	return n
}

func (e *env) duration(key string, def time.Duration) time.Duration {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		e.fail(fmt.Sprintf("%s=%q is not a duration", key, v))
	}
	return d
}

func (e *env) flag(key string) bool {
	switch strings.ToLower(e.str(key, "")) {
	case "", "0", "false", "no":
		return false
	}
	return true
}

func (e *env) list(key string) []string {
	var out []string
	for _, item := range strings.Split(e.str(key, ""), ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func (e *env) fail(msg string) { e.errs = append(e.errs, errors.New(msg)) }

func (e *env) err() error { return errors.Join(e.errs...) }
