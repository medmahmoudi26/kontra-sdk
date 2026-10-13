package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"

	kontradc "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer/dynamicconfig"
)

// pinned are the dynamic config settings the isolation policy leans on (all but the
// header lists are Temporal's defaults). A file that sets one of them to anything else,
// under any constraint, is refused at startup; and while the server runs, pinnedClient serves these
// values whatever the file says, because Temporal re-reads the file and would apply a
// change (the gate edits it for per-namespace limits, decisions I12).
var pinned = map[string]any{
	// Workflow commands that target another namespace (child, signal, cancel) are refused
	// by history. With them on, Temporal would also ask the authorizer about the target
	// namespace, which Kontra's denies; keeping them off removes the path entirely.
	"system.enableCrossNamespaceCommands": false,
	// Streaming RPCs (replication streams, reflection) go through the authorizer.
	"system.disableStreamingAuthorizer": false,
	// A task token from namespace B is refused in a request that names namespace A.
	"frontend.enableTokenNamespaceEnforcement": true,
	// Nexus headers a caller may not set, for workflow callers (HSM) and standalone
	// operations (CHASM).
	"component.nexusoperations.disallowedHeaders": disallowedNexusHeaders,
	"nexusoperation.disallowedHeaders":            disallowedNexusHeaders,
}

// disallowedNexusHeaders is Temporal's default list plus the headers its Nexus HTTP
// transport reads back as the callback token, the request id and the links: a caller's
// header is set after Temporal's own and would replace what the handler sees (v1.32.1
// common/nexus/nexusrpc/client.go). A caller could otherwise hand a seller's handler a
// callback token of its choosing; callbackguard makes that harmless, this makes it
// impossible.
var disallowedNexusHeaders = []any{
	"request-timeout", "xdc-redirection-api", "xdc-redirection-source-cell", "xdc-redirection",
	"caller-name", "caller-type", "call-initiation", "temporal-principal-type", "temporal-principal-name",
	"nexus-callback-temporal-callback-token", "nexus-request-id", "nexus-link",
}

var pinnedValues = func() map[dynamicconfig.Key]any {
	values := make(map[dynamicconfig.Key]any, len(pinned))
	for name, value := range pinned {
		values[dynamicconfig.MakeKey(name)] = value
	}
	return values
}()

// dynamicConfigFile returns the dynamic config file the server polls, after checking it:
// the configured one, or a copy of the defaults built into this binary.
func dynamicConfigFile(path string) (string, error) {
	contents := kontradc.Default
	if path != "" {
		var err error
		if contents, err = os.ReadFile(path); err != nil {
			return "", err
		}
	}
	if err := checkDynamicConfig(contents); err != nil {
		return "", fmt.Errorf("dynamic config %s: %w", orDefault(path), err)
	}
	if path != "" {
		return path, nil
	}
	f, err := os.CreateTemp("", "kontra-dynamicconfig-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = f.Write(contents)
	return f.Name(), err
}

// checkDynamicConfig refuses a file Temporal would only warn about: an unknown key (a
// typo silently keeps the default), a value of the wrong type, or a pinned setting
// changed. Temporal re-reads the file while running and only logs such problems then, so
// whoever edits it (the gate, for per-namespace limits) must keep it valid; the pinned
// settings hold regardless (pinnedClient).
func checkDynamicConfig(contents []byte) error {
	loaded := dynamicconfig.LoadYamlFile(contents)
	problems := append(loaded.Errors, loaded.Warnings...)
	for key, want := range pinned {
		for _, cv := range loaded.Map[dynamicconfig.MakeKey(key)] {
			if !reflect.DeepEqual(cv.Value, want) {
				problems = append(problems, fmt.Errorf("%s must be %v, not %v", key, want, cv.Value))
			}
		}
	}
	return errors.Join(problems...)
}

// notifyingClient is what Temporal's file-based client is: values plus change updates.
type notifyingClient interface {
	dynamicconfig.Client
	dynamicconfig.NotifyingClient
}

// pinnedClient serves the pinned settings from pinned, and everything else from the file.
type pinnedClient struct{ file notifyingClient }

var _ notifyingClient = pinnedClient{}

// newDynamicConfigClient is the client the server reads its dynamic config from.
func newDynamicConfigClient(path string, poll time.Duration, logger log.Logger) (pinnedClient, error) {
	file, err := dynamicconfig.NewFileBasedClient(
		&dynamicconfig.FileBasedClientConfig{Filepath: path, PollInterval: poll},
		logger, make(chan any), // polls for the life of the process
	)
	if err != nil {
		return pinnedClient{}, err
	}
	return pinnedClient{file: file}, nil
}

func (c pinnedClient) GetValue(key dynamicconfig.Key) []dynamicconfig.ConstrainedValue {
	if value, ok := pinnedValues[key]; ok {
		return []dynamicconfig.ConstrainedValue{{Value: value}}
	}
	return c.file.GetValue(key)
}

func (c pinnedClient) Subscribe(update dynamicconfig.ClientUpdateFunc) (cancel func()) {
	return c.file.Subscribe(func(changed map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue) {
		unpinned := make(map[dynamicconfig.Key][]dynamicconfig.ConstrainedValue, len(changed))
		for key, values := range changed {
			if _, ok := pinnedValues[key]; !ok {
				unpinned[key] = values
			}
		}
		if len(unpinned) > 0 {
			update(unpinned)
		}
	})
}

func orDefault(path string) string {
	if path == "" {
		return "(built-in default)"
	}
	return path
}
