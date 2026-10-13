// Command kontra-temporal is Kontra's Temporal server: every Temporal service in one
// process, a public frontend guarded by the Kontra authorizer and Temporal's JWT claim
// mapper, and an internal frontend for the gate and for Temporal itself (PRD §10.2).
//
//	kontra-temporal [serve]                       run the server (configured by KONTRA_* variables)
//	kontra-temporal setup-schema                  create or update the Postgres schemas, then exit
//	kontra-temporal register-namespace NAME...    register namespaces through the internal frontend
//
// README.md lists every variable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // Postgres driver
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"     // SQLite driver
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/temporal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	kontra "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer"
	"github.com/medmahmoudi26/kontra-sdk/temporal/authorizer/callbackguard"
	"github.com/medmahmoudi26/kontra-sdk/temporal/authorizer/revocation"
)

const usage = `usage:
  kontra-temporal [serve]                     run the server (KONTRA_* variables, see README.md)
  kontra-temporal setup-schema                create or update the Postgres schemas and exit
  kontra-temporal register-namespace [-address host:port] [-retention 72h] NAME...
`

func main() {
	command, args := "serve", os.Args[1:]
	if len(args) > 0 {
		command, args = args[0], args[1:]
	}
	var err error
	switch command {
	case "serve":
		err = serve()
	case "setup-schema":
		err = setupSchemaOnly()
	case "register-namespace":
		err = registerNamespaces(args)
	case "help", "-h", "-help", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kontra-temporal:", err)
		os.Exit(1)
	}
}

func serve() error {
	s, err := loadSettings()
	if err != nil {
		return err
	}
	dcPath, err := dynamicConfigFile(s.dynamicConfig)
	if err != nil {
		return err
	}
	cfg := s.temporalConfig(dcPath)
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger := log.NewZapLogger(log.BuildZapLogger(cfg.Log))
	if len(s.jwks) == 0 {
		logger.Warn("KONTRA_JWKS_URLS is empty: every token will be rejected")
	}
	if s.autoSetup {
		if err := setupSchema(s, logger); err != nil {
			return err
		}
	}
	dynamicConfig, err := newDynamicConfigClient(dcPath, s.dynamicConfigPoll, logger)
	if err != nil {
		return err
	}
	// Worker tokens of the sessions the gate revoked are refused within a refresh, not
	// when they expire (revocation package).
	var revoked *revocation.List
	if s.revocationsURL == "" {
		logger.Warn("no revocation list (KONTRA_REVOCATIONS_URL=off, or a JWKS that is not " +
			"a gate's): a revoked session's worker tokens work until they expire")
	} else {
		revoked = revocation.New(s.revocationsURL, nil, logger)
		ctx, stop := context.WithCancel(context.Background())
		defer stop()
		go revoked.Run(ctx, s.revocationsRefresh)
	}

	server, err := temporal.NewServer(
		temporal.ForServices(temporal.Services), // includes the internal frontend
		temporal.WithConfig(cfg),                // before WithClaimMapper, which reads it
		temporal.WithStaticHosts(s.hosts()),
		temporal.WithLogger(logger),
		temporal.InterruptOn(temporal.InterruptCh()),
		temporal.WithDynamicConfigClient(dynamicConfig), // the file, with the isolation settings pinned
		temporal.WithAuthorizer(kontra.New()),
		// After authorization, on both frontends: completion callbacks only in the shape a
		// Nexus handler needs, so a token completes only its own operation (callbackguard).
		temporal.WithChainedFrontendGrpcInterceptors(callbackguard.Intercept),
		// The public frontend verifies JWTs with Temporal's default claim mapper and key
		// provider (PRD §10.3), and then refuses a token whose session the gate revoked:
		// that wrapper only ever refuses more. The internal frontend ignores this and maps
		// every caller to Temporal's internal principal.
		temporal.WithClaimMapper(func(cfg *config.Config) authorization.ClaimMapper {
			keys := authorization.NewDefaultTokenKeyProvider(&cfg.Global.Authorization, logger)
			mapper := authorization.NewDefaultJWTClaimMapper(keys, &cfg.Global.Authorization, logger)
			if revoked == nil {
				return mapper
			}
			return revocation.Check(mapper, revoked)
		}),
		temporal.WithAudienceGetter(func(cfg *config.Config) authorization.JWTAudienceMapper {
			return authorization.NewAudienceMapper(cfg.Global.Authorization.Audience)
		}),
	)
	if err != nil {
		return err
	}
	public, internal := s.rpc[primitives.FrontendService], s.rpc[primitives.InternalFrontendService]
	logger.Info("kontra-temporal starting",
		tag.NewStringTag("public-frontend", fmt.Sprintf("%s:%d", public.BindOnIP, public.GRPCPort)),
		tag.NewInt("public-frontend-http-port", public.HTTPPort),
		tag.NewStringTag("internal-frontend", fmt.Sprintf("%s:%d", internal.BindOnIP, internal.GRPCPort)),
		tag.NewInt("internal-frontend-http-port", internal.HTTPPort),
		tag.NewStringTag("persistence", s.db),
		tag.NewStringTag("dynamic-config", dcPath),
		tag.NewStringTag("revocations", s.revocationsURL),
	)
	return server.Start() // blocks until SIGINT or SIGTERM
}

func setupSchemaOnly() error {
	s, err := loadSettings()
	if err != nil {
		return err
	}
	if s.db != "postgres" {
		return errors.New("setup-schema needs KONTRA_DB=postgres (SQLite sets its schema up itself)")
	}
	return setupSchema(s, log.NewCLILogger())
}

// registerNamespaces registers namespaces through the internal frontend, which needs no
// token. The gate does this when a party signs up; this command is for tests and dev.
func registerNamespaces(args []string) error {
	s, err := loadSettings()
	if err != nil {
		return err
	}
	flags := flag.NewFlagSet("register-namespace", flag.ExitOnError)
	address := flags.String("address", s.address(primitives.InternalFrontendService, false), "internal frontend host:port")
	retention := flags.Duration("retention", 72*time.Hour, "workflow execution retention")
	_ = flags.Parse(args)
	if flags.NArg() == 0 {
		return errors.New("register-namespace needs at least one NAME")
	}

	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := workflowservice.NewWorkflowServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, name := range flags.Args() {
		_, err := client.RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace:                        name,
			WorkflowExecutionRetentionPeriod: durationpb.New(*retention),
		})
		var exists *serviceerror.NamespaceAlreadyExists
		switch {
		case errors.As(serviceerror.FromStatus(status.Convert(err)), &exists):
			fmt.Println("exists:", name)
		case err != nil:
			return fmt.Errorf("register %s: %w", name, err)
		default:
			fmt.Println("registered:", name)
		}
	}
	return nil
}
