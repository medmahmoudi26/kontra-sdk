package main

import (
	"fmt"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/tools/common/schema"
	sqltool "go.temporal.io/server/tools/sql"
)

// schemaNames maps each Postgres store to the versioned schema embedded in the server.
var schemaNames = map[string]string{
	"postgres":            "postgresql/v12/temporal",
	"postgres-visibility": "postgresql/v12/visibility",
}

// setupSchema creates the Temporal and visibility databases if they are missing and brings
// both schemas up to the version this server embeds, with Temporal's own schema tool (what
// temporal-sql-tool create / setup-schema -v 0.0 / update-schema do). It is idempotent:
// setup skips a database that already has a version, and update applies only newer steps.
func setupSchema(s *settings, logger log.Logger) error {
	if s.db != "postgres" {
		return nil // SQLite sets its schema up when it opens the database.
	}
	for store, schemaName := range schemaNames {
		cfg := *s.pg[store]
		create := cfg // DoCreateDatabase connects to the admin database instead
		if err := sqltool.DoCreateDatabase(&create, s.adminDB, logger); err != nil {
			return fmt.Errorf("create database %s: %w", cfg.DatabaseName, err)
		}
		if err := migrate(&cfg, schemaName, logger); err != nil {
			return fmt.Errorf("schema %s in database %s: %w", schemaName, cfg.DatabaseName, err)
		}
		logger.Info("schema up to date: " + schemaName + " in " + cfg.DatabaseName)
	}
	return nil
}

func migrate(cfg *config.SQL, schemaName string, logger log.Logger) error {
	conn, err := sqltool.NewConnection(cfg, logger)
	if err != nil {
		return err
	}
	defer conn.Close()
	setup := &schema.SetupConfig{InitialVersion: "0.0"}
	if err := schema.NewSetupSchemaTask(conn, setup, logger).Run(); err != nil {
		return err
	}
	update := &schema.UpdateConfig{DBName: cfg.DatabaseName, SchemaName: schemaName}
	return schema.NewUpdateSchemaTask(conn, update, logger).Run()
}
