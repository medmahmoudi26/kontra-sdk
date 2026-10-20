package callbackguard_test

import (
	"context"
	"encoding/base64"
	"errors"
	"slices"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/authorization"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	kontra "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer"
	"github.com/medmahmoudi26/kontra-sdk/temporal/authorizer/callbackguard"
)

// The tokens Temporal issues (components/nexusoperations/executors.go for a workflow
// caller, chasm/lib/nexusoperation/task_handler_helpers.go for a standalone operation),
// and the forgeries a handler can make from its own.
func hsmToken(t *testing.T, requestID string) string {
	t.Helper()
	return tokenize(t, &tokenspb.NexusOperationCompletion{
		NamespaceId: "6c9ed6ca-0000-4000-8000-000000000001",
		WorkflowId:  "compose-1",
		RunId:       "6c9ed6ca-0000-4000-8000-000000000002",
		Ref: &persistencespb.StateMachineRef{
			Path: []*persistencespb.StateMachineKey{{Type: "nexusoperations.Operation", Id: "5"}},
			MachineInitialVersionedTransition: &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1, TransitionCount: 3,
			},
		},
		RequestId: requestID,
	})
}

func chasmToken(t *testing.T, requestID string) string {
	t.Helper()
	ref, err := proto.Marshal(&persistencespb.ChasmComponentRef{
		NamespaceId: "6c9ed6ca-0000-4000-8000-000000000001",
		BusinessId:  "job-op-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return tokenize(t, &tokenspb.NexusOperationCompletion{ComponentRef: ref, RequestId: requestID})
}

func tokenize(t *testing.T, c *tokenspb.NexusOperationCompletion) string {
	t.Helper()
	token, err := commonnexus.NewCallbackTokenGenerator().Tokenize(c)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func callback(url string, header map[string]string) *commonpb.Callback {
	return &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
		Nexus: &commonpb.Callback_Nexus{Url: url, Header: header},
	}}
}

func system(token string) *commonpb.Callback {
	return callback(commonnexus.SystemCallbackURL, map[string]string{commonnexus.CallbackTokenHeader: token})
}

func start(cbs ...*commonpb.Callback) *ws.StartWorkflowExecutionRequest {
	return &ws.StartWorkflowExecutionRequest{Namespace: "other", WorkflowId: "forge", CompletionCallbacks: cbs}
}

func refused(t *testing.T, msg proto.Message) {
	t.Helper()
	var denied *serviceerror.PermissionDenied
	if err := callbackguard.Check(msg); !errors.As(err, &denied) {
		t.Fatalf("want PermissionDenied, got %v", err)
	}
}

func passed(t *testing.T, msg proto.Message) {
	t.Helper()
	if err := callbackguard.Check(msg); err != nil {
		t.Fatalf("want no error, got %v", err)
	}
}

func TestGenuineTokensPass(t *testing.T) {
	passed(t, start(system(hsmToken(t, "8d4c5d2e-request"))))
	passed(t, start(system(chasmToken(t, "8d4c5d2e-request"))))
	passed(t, start()) // no callback at all
	// A guessed request id is not this guard's business: history compares it with the
	// operation's own and completes nothing (the wire suite checks that).
	passed(t, start(system(hsmToken(t, "guessed"))))
}

func TestTokensWithoutRequestIDAreRefused(t *testing.T) {
	for name, token := range map[string]string{
		"workflow caller (HSM)": hsmToken(t, ""),
		"standalone (CHASM)":    chasmToken(t, ""),
		"not JSON":              "not a token",
		"unknown version":       `{"v":2,"d":""}`,
		"not a completion":      `{"v":1,"d":"!!!"}`,
		"empty completion":      `{"v":1,"d":""}`,
		"incomplete HSM fields": tokenize(t, &tokenspb.NexusOperationCompletion{NamespaceId: "x", RequestId: "r"}),
		"empty header value":    "",
	} {
		t.Run(name, func(t *testing.T) { refused(t, start(system(token))) })
	}
}

func TestEverySpellingOfTheHeaderIsChecked(t *testing.T) {
	good, bad := hsmToken(t, "req"), hsmToken(t, "")
	for _, key := range []string{"temporal-callback-token", "TEMPORAL-CALLBACK-TOKEN", "Temporal-Callback-Token"} {
		refused(t, start(callback(commonnexus.SystemCallbackURL, map[string]string{key: bad})))
		passed(t, start(callback(commonnexus.SystemCallbackURL, map[string]string{key: good})))
	}
	// Temporal keeps one of two spellings, at random: two are refused, even both genuine.
	refused(t, start(callback(commonnexus.SystemCallbackURL, map[string]string{
		"temporal-callback-token": good, "Temporal-Callback-Token": bad,
	})))
	refused(t, start(callback(commonnexus.SystemCallbackURL, map[string]string{
		"temporal-callback-token": good, "Temporal-Callback-Token": good,
	})))
	refused(t, start(callback(commonnexus.SystemCallbackURL, nil))) // no token
	// Other headers are not tokens.
	passed(t, start(callback(commonnexus.SystemCallbackURL, map[string]string{
		commonnexus.CallbackTokenHeader: good, "temporal-callback-token-x": "anything",
	})))
}

// TestOnlySystemCallbacks: a callback is an HTTP request Temporal makes, and Temporal does
// not validate its URL on every API that accepts one (UpdateWorkflowExecution, v1.32.1).
func TestOnlySystemCallbacks(t *testing.T) {
	good := hsmToken(t, "req")
	header := map[string]string{commonnexus.CallbackTokenHeader: good}
	for _, url := range []string{
		"http://127.0.0.1:7246/api/v1/namespaces/acme/workflows/x/terminate", // the internal frontend's HTTP API
		"https://example.com/nexus/callback",
		"http://169.254.169.254/latest/meta-data",
		"temporal://system/", "TEMPORAL://system", " temporal://system", "temporal://System",
		chasm.NexusCompletionHandlerURL, // temporal://internal: Temporal's own CHASM callbacks
		"",
	} {
		refused(t, start(callback(url, header)))
	}
	refused(t, start(&commonpb.Callback{Variant: &commonpb.Callback_Internal_{
		Internal: &commonpb.Callback_Internal{Data: []byte("x")},
	}}))
	refused(t, start(&commonpb.Callback{})) // no variant
	// The same callbacks on an Update, which Temporal does not validate at all.
	refused(t, &ws.UpdateWorkflowExecutionRequest{Namespace: "other", Request: &updatepb.Request{
		CompletionCallbacks: []*commonpb.Callback{callback("http://127.0.0.1:7246/api/v1/nexus/endpoints", header)},
	}})
}

func TestTheInternalPrincipalIsNotChecked(t *testing.T) {
	// The CHASM scheduler attaches temporal://internal callbacks with a legacy token (a bare
	// component ref, no request id) through the internal frontend.
	ref, _ := proto.Marshal(&persistencespb.ChasmComponentRef{NamespaceId: "ns", BusinessId: "b"})
	legacy := callback(chasm.NexusCompletionHandlerURL, map[string]string{
		commonnexus.CallbackTokenHeader: base64.RawURLEncoding.EncodeToString(ref),
	})
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	internal, err := authorization.NewInternalClaimMapper().GetClaims(&authorization.AuthInfo{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), authorization.MappedClaims, internal)
	if _, err := callbackguard.Intercept(ctx, start(legacy), nil, handler); err != nil {
		t.Fatalf("the internal principal was refused: %v", err)
	}
	// A JWT caller with the same request, whatever its subject, is checked.
	for _, claims := range []*authorization.Claims{
		nil,
		{AuthType: "jwt", Subject: authorization.InternalPrincipalName, System: authorization.RoleAdmin},
		{AuthType: "jwt", Subject: "other", Namespaces: map[string]authorization.Role{"other": authorization.RoleWorker}},
	} {
		ctx := context.WithValue(context.Background(), authorization.MappedClaims, claims)
		if _, err := callbackguard.Intercept(ctx, start(legacy), nil, handler); err == nil {
			t.Fatalf("%+v was not checked", claims)
		}
	}
}

// TestTheRelayAndTheControllerAreChecked: the stream relay's one write is an Update, which
// can carry completion callbacks; the authorizer allows it by its handler name and workflow
// and never reads a callback. The guard applies to it, and to the worker controller, as to
// any caller that is not Temporal's internal principal.
func TestTheRelayAndTheControllerAreChecked(t *testing.T) {
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	poll := func(cbs ...*commonpb.Callback) *ws.UpdateWorkflowExecutionRequest {
		return &ws.UpdateWorkflowExecutionRequest{
			Namespace:         "acme",
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "job-0123456789abcdef0123456789abcdef"},
			Request: &updatepb.Request{
				Input:               &updatepb.Input{Name: kontra.StreamPollUpdate},
				CompletionCallbacks: cbs,
			},
		}
	}
	for _, claims := range []*authorization.Claims{
		// What Temporal's default claim mapper makes of the relay's and the controller's
		// tokens: no permission, so no namespace and no system role.
		{AuthType: "jwt", Subject: kontra.RelaySubject("acme", "job-0123456789abcdef0123456789abcdef")},
		{AuthType: "jwt", Subject: kontra.ControllerSubject},
	} {
		ctx := context.WithValue(context.Background(), authorization.MappedClaims, claims)
		info := &grpc.UnaryServerInfo{FullMethod: ws.WorkflowService_UpdateWorkflowExecution_FullMethodName}
		var denied *serviceerror.PermissionDenied
		_, err := callbackguard.Intercept(ctx, poll(callback("http://127.0.0.1:7246/api/v1/nexus/endpoints", nil)), info, handler)
		if !errors.As(err, &denied) {
			t.Fatalf("%s: a poll carrying a non-system callback got %v", claims.Subject, err)
		}
		_, err = callbackguard.Intercept(ctx, poll(system(hsmToken(t, ""))), info, handler)
		if !errors.As(err, &denied) {
			t.Fatalf("%s: a poll carrying a forged token got %v", claims.Subject, err)
		}
		if _, err := callbackguard.Intercept(ctx, poll(), info, handler); err != nil {
			t.Fatalf("%s: a plain poll was refused: %v", claims.Subject, err)
		}
	}
}

func TestEveryCarrierIsChecked(t *testing.T) {
	bad := system(hsmToken(t, ""))
	refused(t, start(system(hsmToken(t, "req")), bad)) // not only the first callback
	refused(t, &ws.ExecuteMultiOperationRequest{Namespace: "other", Operations: []*ws.ExecuteMultiOperationRequest_Operation{
		{Operation: &ws.ExecuteMultiOperationRequest_Operation_StartWorkflow{StartWorkflow: start(bad)}},
	}})
	refused(t, &ws.ExecuteMultiOperationRequest{Namespace: "other", Operations: []*ws.ExecuteMultiOperationRequest_Operation{
		{Operation: &ws.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{UpdateWorkflow: &ws.UpdateWorkflowExecutionRequest{
			Request: &updatepb.Request{CompletionCallbacks: []*commonpb.Callback{bad}},
		}}},
	}})
	refused(t, &ws.StartActivityExecutionRequest{Namespace: "other", CompletionCallbacks: []*commonpb.Callback{bad}})
	refused(t, &ws.UpdateWorkflowExecutionRequest{Namespace: "other", Request: &updatepb.Request{
		CompletionCallbacks: []*commonpb.Callback{bad},
	}})
	refused(t, &ws.StartNexusOperationExecutionRequest{Namespace: "other", CompletionCallbacks: []*commonpb.Callback{bad}})
}

// TestCallbackCarriers pins which WorkflowService requests can carry a callback at all. The
// guard walks any request that can, so a new one is covered; this test makes a reviewer
// look at it when Temporal adds one.
//
// It has done its job once: go.temporal.io/api v1.63.6 gave
// StartNexusOperationExecutionRequest a `completion_callbacks` field (18), making the
// standalone-operation start a carrier like the four before it. Nothing in the guard
// changed — it finds carriers by walking the type — and nothing in the Authorizer changed
// either: StartNexusOperationExecution was already in AllowedAPIs, in the token's own
// namespace (decisions I4), and the guard now checks the callbacks it may carry.
func TestCallbackCarriers(t *testing.T) {
	var got []string
	methods := ws.File_temporal_api_workflowservice_v1_service_proto.Services().ByName("WorkflowService").Methods()
	for i := 0; i < methods.Len(); i++ {
		input := methods.Get(i).Input()
		mt, err := protoTypeOf(input)
		if err != nil {
			t.Fatal(err)
		}
		if err := callbackguard.Check(withBadCallbackEverywhere(t, mt.New())); err != nil {
			got = append(got, string(methods.Get(i).Name()))
		}
	}
	want := []string{
		"ExecuteMultiOperation",
		"StartActivityExecution",
		"StartNexusOperationExecution",
		"StartWorkflowExecution",
		"UpdateWorkflowExecution",
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("requests that can carry a completion callback: %v, want %v", got, want)
	}
}

func TestInterceptCallsTheHandlerOnlyWhenClean(t *testing.T) {
	called := 0
	handler := func(context.Context, any) (any, error) { called++; return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: ws.WorkflowService_StartWorkflowExecution_FullMethodName}
	if _, err := callbackguard.Intercept(context.Background(), start(system(hsmToken(t, ""))), info, handler); err == nil || called != 0 {
		t.Fatalf("a forged callback reached the handler (err %v, calls %d)", err, called)
	}
	if res, err := callbackguard.Intercept(context.Background(), start(system(hsmToken(t, "r"))), info, handler); err != nil || res != "ok" || called != 1 {
		t.Fatalf("a genuine callback was refused: %v", err)
	}
	if _, err := callbackguard.Intercept(context.Background(), "not a proto", info, handler); err != nil || called != 2 {
		t.Fatalf("a non-proto request was refused: %v", err)
	}
}

// --- helpers ------------------------------------------------------------------------

func protoTypeOf(md protoreflect.MessageDescriptor) (protoreflect.MessageType, error) {
	return protoregistry.GlobalTypes.FindMessageByName(md.FullName())
}

// withBadCallbackEverywhere fills every message field reachable from m (to a bounded
// depth of 4, enough for ExecuteMultiOperation's 2) and puts a forged callback in every Callback list it finds.
func withBadCallbackEverywhere(t *testing.T, m protoreflect.Message) proto.Message {
	t.Helper()
	fill(m, system(hsmToken(t, "")), 0)
	return m.Interface()
}

func fill(m protoreflect.Message, bad *commonpb.Callback, depth int) {
	if depth > 4 {
		return
	}
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.Message() == nil || fd.IsMap() {
			continue
		}
		if fd.Message().FullName() == bad.ProtoReflect().Descriptor().FullName() {
			if fd.IsList() {
				m.Mutable(fd).List().Append(protoreflect.ValueOfMessage(proto.Clone(bad).ProtoReflect()))
			} else {
				m.Set(fd, protoreflect.ValueOfMessage(proto.Clone(bad).ProtoReflect()))
			}
			continue
		}
		if fd.ContainingOneof() != nil && m.WhichOneof(fd.ContainingOneof()) != nil {
			continue
		}
		if fd.IsList() {
			elem := m.Mutable(fd).List().AppendMutable().Message()
			fill(elem, bad, depth+1)
			continue
		}
		fill(m.Mutable(fd).Message(), bad, depth+1)
	}
}
