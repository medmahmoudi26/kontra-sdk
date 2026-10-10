package main

import (
	"slices"
	"strings"
	"testing"

	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/components/nexusoperations"

	kontradc "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer/dynamicconfig"
)

// clearEnv unsets every KONTRA_ variable for the test, so the host's environment cannot
// change the defaults under test.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"KONTRA_PORT_BASE", "KONTRA_FRONTEND_BIND_IP", "KONTRA_FRONTEND_HTTP_PORT",
		"KONTRA_INTERNAL_FRONTEND_BIND_IP", "KONTRA_INTERNAL_FRONTEND_HTTP_PORT", "KONTRA_DB",
		"KONTRA_SQLITE_FILE", "KONTRA_FRONTEND_PORT", "KONTRA_INTERNAL_FRONTEND_PORT",
		"KONTRA_HISTORY_PORT", "KONTRA_MATCHING_PORT", "KONTRA_WORKER_PORT", "KONTRA_JWKS_URLS",
		"KONTRA_JWT_AUDIENCE",
	} {
		t.Setenv(key, "")
	}
}

func TestDefaultsListenOnLoopbackOnly(t *testing.T) {
	clearEnv(t)
	s, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}
	for name, rpc := range s.rpc {
		if rpc.BindOnIP != "127.0.0.1" {
			t.Errorf("%s binds %q by default", name, rpc.BindOnIP)
		}
	}
	if port := s.rpc[primitives.FrontendService].HTTPPort; port != 0 {
		t.Errorf("the public HTTP API must be off by default, got port %d", port)
	}
	if port := s.rpc[primitives.InternalFrontendService].HTTPPort; port == 0 {
		t.Error("the internal frontend must serve HTTP (Nexus dispatch and completions)")
	}
	for name, hosts := range s.hosts() {
		if !strings.HasPrefix(hosts.Self, "127.0.0.1:") || len(hosts.All) != 1 || hosts.All[0] != hosts.Self {
			t.Errorf("%s: static host %+v", name, hosts)
		}
	}
	if got := s.rpc[primitives.FrontendService].GRPCPort; got != 7233 {
		t.Errorf("frontend port %d", got)
	}
	if got := s.rpc[primitives.InternalFrontendService].GRPCPort; got != 7236 {
		t.Errorf("internal frontend port %d", got)
	}
}

func TestOnlyTheFrontendsCanLeaveLoopback(t *testing.T) {
	clearEnv(t)
	t.Setenv("KONTRA_FRONTEND_BIND_IP", "0.0.0.0")
	t.Setenv("KONTRA_INTERNAL_FRONTEND_BIND_IP", "0.0.0.0")
	s, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}
	for name, rpc := range s.rpc {
		frontend := name == primitives.FrontendService || name == primitives.InternalFrontendService
		if frontend != (rpc.BindOnIP == "0.0.0.0") {
			t.Errorf("%s binds %q", name, rpc.BindOnIP)
		}
	}
	for name, hosts := range s.hosts() {
		if !strings.HasPrefix(hosts.Self, "127.0.0.1:") {
			t.Errorf("%s: services must reach each other over loopback, got %s", name, hosts.Self)
		}
	}
}

func TestBadSettingsAreReportedTogether(t *testing.T) {
	clearEnv(t)
	t.Setenv("KONTRA_INTERNAL_FRONTEND_HTTP_PORT", "0")
	t.Setenv("KONTRA_DB", "mysql")
	t.Setenv("KONTRA_PORT_BASE", "x")
	_, err := loadSettings()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"INTERNAL_FRONTEND_HTTP_PORT", "KONTRA_DB", "KONTRA_PORT_BASE"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s: %v", want, err)
		}
	}
}

func TestConfigValidates(t *testing.T) {
	for _, db := range []string{"sqlite", "postgres"} {
		clearEnv(t)
		t.Setenv("KONTRA_DB", db)
		s, err := loadSettings()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.temporalConfig("/dev/null").Validate(); err != nil {
			t.Errorf("%s: %v", db, err)
		}
	}
}

func TestDefaultDynamicConfigIsValid(t *testing.T) {
	if err := checkDynamicConfig(kontradc.Default); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"nexusoperation.enableStandalone", "history.enableChasm", "system.enableCrossNamespaceCommands"} {
		if !strings.Contains(string(kontradc.Default), want+":") {
			t.Errorf("default dynamic config lacks %s", want)
		}
	}
}

func TestDynamicConfigIsCheckedStrictly(t *testing.T) {
	for name, yaml := range map[string]string{
		"typo":                            "frontend.namespaceRSP:\n  - value: 10\n",
		"wrong type":                      "frontend.namespaceRPS:\n  - value: lots\n",
		"cross-namespace on":              "system.enableCrossNamespaceCommands:\n  - value: true\n",
		"cross-namespace on (ns)":         "system.enableCrossNamespaceCommands:\n  - value: false\n  - value: true\n    constraints: {namespace: acme}\n",
		"streams unauthorized":            "system.disableStreamingAuthorizer:\n  - value: true\n",
		"token namespaces off":            "frontend.enableTokenNamespaceEnforcement:\n  - value: false\n",
		"caller sets callback":            "nexusoperation.disallowedHeaders:\n  - value: [request-timeout]\n",
		"caller sets callback (workflow)": "component.nexusoperations.disallowedHeaders:\n  - value: []\n",
	} {
		if err := checkDynamicConfig([]byte(yaml)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ok := "frontend.namespaceRPS:\n  - value: 1000\n    constraints: {namespace: acme}\n  - value: 200\n"
	if err := checkDynamicConfig([]byte(ok)); err != nil {
		t.Errorf("a per-namespace override was refused: %v", err)
	}
}

// TestAudienceIsAlwaysChecked: Temporal's claim mapper skips the "aud" check when no
// audience is configured, and the gate signs API, hold and browser tokens with the same
// keys as worker tokens. An empty or unset variable must not turn the check off.
func TestAudienceIsAlwaysChecked(t *testing.T) {
	clearEnv(t)
	s, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}
	if s.audience != "kontra:temporal" {
		t.Fatalf("default audience %q", s.audience)
	}
	if got := s.temporalConfig("/dev/null").Global.Authorization.Audience; got != "kontra:temporal" {
		t.Fatalf("the server is configured with audience %q", got)
	}
	t.Setenv("KONTRA_JWT_AUDIENCE", "kontra:temporal-staging")
	if s, _ := loadSettings(); s.audience != "kontra:temporal-staging" {
		t.Fatalf("audience %q", s.audience)
	}
}

// TestPinnedSettingsHoldAtRuntime: Temporal re-reads the dynamic config file while running
// and applies what it finds; the startup check alone would not stop a later edit from
// turning cross-namespace commands on. Read through Temporal's own accessors.
func TestPinnedSettingsHoldAtRuntime(t *testing.T) {
	file := dynamicconfig.NewMemoryClient()
	file.OverrideValue(dynamicconfig.EnableCrossNamespaceCommands.Key(), true)
	file.OverrideValue(dynamicconfig.DisableStreamingAuthorizer.Key(), true)
	file.OverrideValue(dynamicconfig.EnableTokenNamespaceEnforcement.Key(), false)
	file.OverrideValue(dynamicconfig.FrontendGlobalNamespaceRPS.Key(), 7)
	unpinned := dynamicconfig.NewCollection(file, log.NewNoopLogger())
	if !dynamicconfig.EnableCrossNamespaceCommands.Get(unpinned)() {
		t.Fatal("control: Temporal reads the file's value without the pin")
	}
	collection := dynamicconfig.NewCollection(pinnedClient{file: file}, log.NewNoopLogger())
	collection.Start()
	defer collection.Stop()

	if dynamicconfig.EnableCrossNamespaceCommands.Get(collection)() {
		t.Error("cross-namespace commands turned on at runtime")
	}
	if dynamicconfig.DisableStreamingAuthorizer.Get(collection)() {
		t.Error("streaming authorizer turned off at runtime")
	}
	if !dynamicconfig.EnableTokenNamespaceEnforcement.Get(collection)() {
		t.Error("token namespace enforcement turned off at runtime")
	}
	for name, headers := range map[string][]string{
		"workflow callers": nexusoperations.DisallowedOperationHeaders.Get(collection)(),
		"standalone":       chasmnexus.DisallowedOperationHeaders.Get(collection)(),
	} {
		for _, h := range []string{"nexus-callback-temporal-callback-token", "nexus-request-id", "nexus-link", "request-timeout"} {
			if !slices.Contains(headers, h) {
				t.Errorf("%s: a caller may set %s: %v", name, h, headers)
			}
		}
	}
	if got := dynamicconfig.FrontendGlobalNamespaceRPS.Get(collection)("acme"); got != 7 {
		t.Errorf("an unpinned setting must come from the file, got %d", got)
	}
	for name := range pinned {
		if _, ok := pinnedValues[dynamicconfig.MakeKey(name)]; !ok {
			t.Errorf("%s is not served pinned", name)
		}
	}

	// Change notifications for pinned keys are dropped; others pass.
	var seen []dynamicconfig.Key
	cancel := pinnedClient{file: file}.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
		for key := range changed {
			seen = append(seen, key)
		}
	})
	defer cancel()
	file.OverrideValue(dynamicconfig.EnableCrossNamespaceCommands.Key(), true)
	file.OverrideValue(dynamicconfig.FrontendGlobalNamespaceRPS.Key(), 9)
	for _, key := range seen {
		if _, ok := pinnedValues[key]; ok {
			t.Errorf("a change to pinned %s was passed on", key)
		}
	}
	if len(seen) == 0 {
		t.Error("a change to an unpinned setting was not passed on")
	}
}

func TestPinnedFileValuesAreComparedSafely(t *testing.T) {
	// A list where a bool is pinned must be refused, not crash the comparison.
	for _, yaml := range []string{
		"system.enableCrossNamespaceCommands:\n  - value: [true]\n",
		"system.enableCrossNamespaceCommands:\n  - value: {a: 1}\n",
	} {
		if err := checkDynamicConfig([]byte(yaml)); err == nil {
			t.Errorf("accepted %q", yaml)
		}
	}
}
