package authorizer_test

// The deny matrix (PRD §10.4, decisions I3, acceptance criterion 2) as one table.
//
// Every cell names a caller, an API and a namespace, and the decision of three
// authorizers: Kontra's, Temporal's no-op authorizer (what a server without an authorizer
// does) and Temporal's default authorizer. A deny cell whose noop column says allow is a
// failing-by-default test: without Kontra's authorizer the call goes through. The default
// column shows which cells Temporal's own role model would also miss.
//
// Callers are built by Temporal's real default JWT claim mapper from the permission
// strings a token carries, so "temporal-system:admin" becomes a system role exactly as it
// does on the wire.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	ops "go.temporal.io/api/operatorservice/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	adm "go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoreflect"

	kontra "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer"
)

const (
	allow = authorization.DecisionAllow
	deny  = authorization.DecisionDeny

	nexusPrefix = api.NexusServicePrefix
)

// API names used by the cells.
var (
	poll            = ws.WorkflowService_PollWorkflowTaskQueue_FullMethodName
	pollActivity    = ws.WorkflowService_PollActivityTaskQueue_FullMethodName
	pollNexus       = ws.WorkflowService_PollNexusTaskQueue_FullMethodName
	respondNexus    = ws.WorkflowService_RespondNexusTaskCompleted_FullMethodName
	respondWFT      = ws.WorkflowService_RespondWorkflowTaskCompleted_FullMethodName
	start           = ws.WorkflowService_StartWorkflowExecution_FullMethodName
	signal          = ws.WorkflowService_SignalWorkflowExecution_FullMethodName
	query           = ws.WorkflowService_QueryWorkflow_FullMethodName
	describe        = ws.WorkflowService_DescribeWorkflowExecution_FullMethodName
	history         = ws.WorkflowService_GetWorkflowExecutionHistory_FullMethodName
	list            = ws.WorkflowService_ListWorkflowExecutions_FullMethodName
	count           = ws.WorkflowService_CountWorkflowExecutions_FullMethodName
	cancel          = ws.WorkflowService_RequestCancelWorkflowExecution_FullMethodName
	terminate       = ws.WorkflowService_TerminateWorkflowExecution_FullMethodName
	describeNS      = ws.WorkflowService_DescribeNamespace_FullMethodName
	startOp         = ws.WorkflowService_StartNexusOperationExecution_FullMethodName
	describeOp      = ws.WorkflowService_DescribeNexusOperationExecution_FullMethodName
	pollOp          = ws.WorkflowService_PollNexusOperationExecution_FullMethodName
	systemInfo      = ws.WorkflowService_GetSystemInfo_FullMethodName
	clusterInfo     = ws.WorkflowService_GetClusterInfo_FullMethodName
	registerNS      = ws.WorkflowService_RegisterNamespace_FullMethodName
	updateNS        = ws.WorkflowService_UpdateNamespace_FullMethodName
	deprecateNS     = ws.WorkflowService_DeprecateNamespace_FullMethodName
	listNS          = ws.WorkflowService_ListNamespaces_FullMethodName
	searchAttrs     = ws.WorkflowService_GetSearchAttributes_FullMethodName
	batch           = ws.WorkflowService_StartBatchOperation_FullMethodName
	deleteWF        = ws.WorkflowService_DeleteWorkflowExecution_FullMethodName
	resetWF         = ws.WorkflowService_ResetWorkflowExecution_FullMethodName
	schedule        = ws.WorkflowService_CreateSchedule_FullMethodName
	versioning      = ws.WorkflowService_UpdateWorkerVersioningRules_FullMethodName
	describeTQ      = ws.WorkflowService_DescribeTaskQueue_FullMethodName
	deleteOp        = ws.WorkflowService_DeleteNexusOperationExecution_FullMethodName
	createEndpoint  = ops.OperatorService_CreateNexusEndpoint_FullMethodName
	updateEndpoint  = ops.OperatorService_UpdateNexusEndpoint_FullMethodName
	deleteEndpoint  = ops.OperatorService_DeleteNexusEndpoint_FullMethodName
	getEndpoint     = ops.OperatorService_GetNexusEndpoint_FullMethodName
	listEndpoints   = ops.OperatorService_ListNexusEndpoints_FullMethodName
	listClusters    = ops.OperatorService_ListClusters_FullMethodName
	addSearchAttr   = ops.OperatorService_AddSearchAttributes_FullMethodName
	deleteNS        = ops.OperatorService_DeleteNamespace_FullMethodName
	describeCluster = adm.AdminService_DescribeCluster_FullMethodName
	replication     = adm.AdminService_StreamWorkflowReplicationMessages_FullMethodName
	healthCheck     = healthpb.Health_Check_FullMethodName
	healthWatch     = healthpb.Health_Watch_FullMethodName
	reflection      = reflectionpb.ServerReflection_ServerReflectionInfo_FullMethodName
	dispatchNexus   = nexusPrefix + "DispatchNexusTask"
	dispatchByEP    = nexusPrefix + "DispatchByEndpoint"
	dispatchByTQ    = nexusPrefix + "DispatchByNamespaceAndTaskQueue"
	completeNexus   = nexusPrefix + "CompleteNexusOperation"
	completeChasm   = nexusPrefix + "CompleteNexusOperationChasm"
)

// Callers. Each JWT caller is the claims Temporal's default JWT claim mapper produces for
// a token carrying these permissions.
var (
	internal      = internalClaims()
	anonymous     = (*authorization.Claims)(nil)
	acmeWorker    = mustJWT("seller", "acme:worker")
	buyerWorker   = mustJWT("buyer", "buyer:worker")
	otherWorker   = mustJWT("other", "other:worker")
	acmeAdmin     = mustJWT("seller", "acme:admin")
	acmeWrite     = mustJWT("seller", "acme:write")
	acmeRead      = mustJWT("seller", "acme:read")
	acmeAll       = mustJWT("seller", "acme:worker", "acme:admin", "acme:write", "acme:read")
	systemAdmin   = mustJWT("operator", "temporal-system:admin")
	systemWorker  = mustJWT("operator", "temporal-system:worker")
	fakeInternal  = mustJWT("internal", "temporal-system:admin") // a JWT whose subject imitates the internal principal
	buyerSysRead  = mustJWT("buyer", "buyer:worker", "temporal-system:read")
	buyerSysAdmin = mustJWT("buyer", "buyer:worker", "temporal-system:admin")
	buyerAcmeRead = mustJWT("buyer", "buyer:worker", "acme:read")
	upperWorker   = mustJWT("seller", "ACME:worker")
	acmeWORKER    = mustJWT("seller", "acme:WORKER")
	// What the default claim mapper returns for an empty token: claims, but no roles.
	emptyJWT = &authorization.Claims{AuthType: "jwt"}
	// Not producible by the default claim mapper (it maps "temporal-system:..." to the system
	// role), kept to pin the explicit refusal of Temporal's own namespace.
	sysNSWorker = &authorization.Claims{AuthType: "jwt", Subject: "x",
		Namespaces: map[string]authorization.Role{"temporal-system": authorization.RoleWorker}}
)

type cell struct {
	row    string // the §10.4 row (or decisions I3 rule) this cell belongs to
	who    string
	claims *authorization.Claims
	api    string
	ns     string
	// Decisions of: Kontra's authorizer, Temporal's no-op authorizer, Temporal's default authorizer.
	kontra, noop, dflt authorization.Decision
}

func (c cell) name() string {
	ns := c.ns
	if ns == "" {
		ns = `""`
	}
	return fmt.Sprintf("%s/%s/%s@%s", c.row, c.who, api.MethodName(c.api), ns)
}

var matrix = []cell{
	// --- worker in namespace A: allowed in A -----------------------------------------
	// Temporal's default authorizer ranks "worker" below "read", so it denies the worker
	// role everything: the third column of these rows is why Kontra cannot use it.
	{"worker-A", "acme:worker", acmeWorker, poll, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, pollActivity, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, pollNexus, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, respondNexus, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, respondWFT, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, start, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, signal, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, query, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describe, "acme", allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "acme", allow, allow, deny},
	{"worker-A", "acme:worker + every role", acmeAll, poll, "acme", allow, allow, allow},
	// --- worker in namespace A: denied anything in B -----------------------------------
	{"worker-A", "acme:worker", acmeWorker, start, "buyer", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describe, "buyer", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, signal, "buyer", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, poll, "other", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeOp, "buyer", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "buyer", deny, allow, deny},
	// --- worker in namespace A: denied all Operator and Admin APIs ---------------------
	{"worker-A", "acme:worker", acmeWorker, listClusters, "", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, addSearchAttr, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteNS, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeCluster, "", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, replication, "", deny, allow, deny},
	// --- worker in namespace A: denied the Nexus HTTP API (DispatchNexusTask) ----------
	{"worker-A", "acme:worker", acmeWorker, dispatchNexus, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, dispatchByEP, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, dispatchByTQ, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, completeNexus, "buyer", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, completeChasm, "buyer", deny, allow, deny},
	// --- worker in namespace A: denied namespace administration and unneeded APIs ------
	{"worker-A", "acme:worker", acmeWorker, updateNS, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deprecateNS, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, registerNS, "acme2", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, batch, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteWF, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, resetWF, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, schedule, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, versioning, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeTQ, "acme", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteOp, "acme", deny, allow, deny},
	// --- worker in namespace A: denied calls that name no namespace --------------------
	{"worker-A", "acme:worker", acmeWorker, listNS, "", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, searchAttrs, "", deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "", deny, allow, deny}, // DescribeNamespace by id
	{"worker-A", "acme:worker", acmeWorker, start, "", deny, allow, deny},
	// --- namespace keys are compared exactly -------------------------------------------
	{"exact-namespace", "acme:worker", acmeWorker, start, "ACME", deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme ", deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme\n", deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "\u0430cme", deny, allow, deny},                // Cyrillic a
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme\u200b", deny, allow, deny},               // zero-width space
	{"exact-namespace", "acme:worker", acmeWorker, start, "\uff41\uff43\uff4d\uff45", deny, allow, deny}, // full width
	{"exact-namespace", "ACME:worker", upperWorker, start, "acme", deny, allow, deny},
	{"exact-namespace", "acme:WORKER (roles are case-insensitive in Temporal)", acmeWORKER, start, "acme", allow, allow, deny},
	{"exact-namespace", "temporal-system worker", sysNSWorker, start, "temporal-system", deny, allow, deny},

	// --- any token: no Nexus endpoint API (the publisher role is gone, decisions I3) ---
	{"endpoints", "acme:worker", acmeWorker, createEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, updateEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, deleteEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, getEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, listEndpoints, "", deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, createEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, updateEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, deleteEndpoint, "", deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, listEndpoints, "", deny, allow, deny},
	{"endpoints", "temporal-system:admin", systemAdmin, createEndpoint, "", deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, updateEndpoint, "", deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, deleteEndpoint, "", deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, getEndpoint, "", deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, listEndpoints, "", deny, allow, allow},
	{"endpoints", "buyer:worker", buyerWorker, createEndpoint, "", deny, allow, deny},
	{"endpoints", "buyer:worker", buyerWorker, listEndpoints, "", deny, allow, deny},

	// --- buyer in namespace C: allowed to start workflows and call endpoints in C -------
	{"buyer-C", "buyer:worker", buyerWorker, start, "buyer", allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, startOp, "buyer", allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeOp, "buyer", allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollOp, "buyer", allow, allow, deny},
	// --- buyer in namespace C: denied describing or listing anything in A ---------------
	{"buyer-C", "buyer:worker", buyerWorker, describe, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, query, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, history, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, list, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, count, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, signal, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, cancel, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, terminate, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeOp, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeNS, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, startOp, "acme", deny, allow, deny},
	// --- a worker of C cannot poll A's task queues --------------------------------------
	{"buyer-C", "buyer:worker", buyerWorker, poll, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollActivity, "acme", deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollNexus, "acme", deny, allow, deny},
	{"buyer-C", "other:worker", otherWorker, pollNexus, "acme", deny, allow, deny},
	// --- extra roles in a buyer token grant nothing in A (the default authorizer leaks) ---
	{"buyer-C", "buyer:worker+temporal-system:read", buyerSysRead, describe, "acme", deny, allow, allow},
	{"buyer-C", "buyer:worker+temporal-system:read", buyerSysRead, list, "acme", deny, allow, allow},
	{"buyer-C", "buyer:worker+temporal-system:admin", buyerSysAdmin, pollNexus, "acme", deny, allow, allow},
	{"buyer-C", "buyer:worker+acme:read", buyerAcmeRead, describe, "acme", deny, allow, allow},

	// --- control plane: everything, but only as the internal principal -----------------
	{"control-plane", "internal", internal, createEndpoint, "", allow, allow, allow},
	{"control-plane", "internal", internal, listEndpoints, "", allow, allow, allow},
	{"control-plane", "internal", internal, registerNS, "newparty", allow, allow, allow},
	{"control-plane", "internal", internal, updateNS, "acme", allow, allow, allow},
	{"control-plane", "internal", internal, describe, "acme", allow, allow, allow},
	{"control-plane", "internal", internal, describeCluster, "", allow, allow, allow},
	{"control-plane", "internal", internal, replication, "", allow, allow, allow},
	{"control-plane", "internal", internal, dispatchByEP, "acme", allow, allow, allow}, // history's Nexus dispatch
	{"control-plane", "internal", internal, completeNexus, "buyer", allow, allow, allow},
	// --- a JWT is never the control plane, whatever its roles or subject ----------------
	{"control-plane", "temporal-system:admin", systemAdmin, listClusters, "", deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, describeCluster, "", deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, registerNS, "newparty", deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, deleteNS, "acme", deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, start, "acme", deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, dispatchByEP, "acme", deny, allow, allow},
	{"control-plane", "temporal-system:worker", systemWorker, poll, "acme", deny, allow, deny},
	{"control-plane", "jwt sub=internal", fakeInternal, createEndpoint, "", deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, updateNS, "acme", deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, deleteNS, "acme", deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, addSearchAttr, "acme", deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, listClusters, "", deny, allow, deny},

	// --- roles other than worker grant nothing, even in their own namespace ------------
	{"other-roles", "acme:admin", acmeAdmin, poll, "acme", deny, allow, allow},
	{"other-roles", "acme:admin", acmeAdmin, start, "acme", deny, allow, allow},
	{"other-roles", "acme:write", acmeWrite, start, "acme", deny, allow, allow},
	{"other-roles", "acme:write", acmeWrite, batch, "acme", deny, allow, allow},
	{"other-roles", "acme:read", acmeRead, describe, "acme", deny, allow, allow},
	{"other-roles", "acme:worker+every role", acmeAll, batch, "acme", deny, allow, allow},
	{"other-roles", "acme:worker+every role", acmeAll, updateNS, "acme", deny, allow, allow},

	// --- no token: health checks and system info only ----------------------------------
	{"no-token", "none", anonymous, healthCheck, "", allow, allow, allow},
	{"no-token", "none", anonymous, systemInfo, "", allow, allow, allow},
	{"no-token", "none", anonymous, clusterInfo, "", allow, allow, deny},
	{"no-token", "none", anonymous, start, "acme", deny, allow, deny},
	{"no-token", "none", anonymous, describeNS, "acme", deny, allow, deny},
	{"no-token", "none", anonymous, listEndpoints, "", deny, allow, deny},
	{"no-token", "none", anonymous, describeCluster, "", deny, allow, deny},
	{"no-token", "none", anonymous, dispatchByEP, "acme", deny, allow, deny},
	{"no-token", "none", anonymous, completeNexus, "buyer", deny, allow, deny},
	{"no-token", "none", anonymous, healthWatch, "", deny, allow, deny},
	{"no-token", "none", anonymous, reflection, "", deny, allow, deny},
	{"any-token", "acme:worker", acmeWorker, systemInfo, "", allow, allow, allow},
	{"any-token", "acme:worker", acmeWorker, clusterInfo, "", allow, allow, deny},
	{"any-token", "acme:worker", acmeWorker, healthCheck, "", allow, allow, allow},
	{"any-token", "acme:worker", acmeWorker, reflection, "", deny, allow, deny},
}

func TestDenyMatrix(t *testing.T) {
	authorizers := []struct {
		name   string
		a      authorization.Authorizer
		expect func(cell) authorization.Decision
	}{
		{"kontra", kontra.New(), func(c cell) authorization.Decision { return c.kontra }},
		{"noop", authorization.NewNoopAuthorizer(), func(c cell) authorization.Decision { return c.noop }},
		{"default", authorization.NewDefaultAuthorizer(), func(c cell) authorization.Decision { return c.dflt }},
	}
	for _, c := range matrix {
		for _, az := range authorizers {
			t.Run(az.name+"/"+c.name(), func(t *testing.T) {
				got := decide(t, az.a, c.claims, c.api, c.ns)
				if want := az.expect(c); got != want {
					t.Fatalf("%s authorizer: got %s, want %s", az.name, show(got), show(want))
				}
			})
		}
	}
}

// TestDenyCellsFailByDefault pins acceptance criterion 2: every cell Kontra denies is a
// call Temporal would allow without it.
func TestDenyCellsFailByDefault(t *testing.T) {
	denied := 0
	for _, c := range matrix {
		if c.kontra != deny {
			continue
		}
		denied++
		if c.noop != allow {
			t.Errorf("%s: a deny cell must be allowed by the no-op authorizer", c.name())
		}
		if got := decide(t, authorization.NewNoopAuthorizer(), c.claims, c.api, c.ns); got != allow {
			t.Errorf("%s: the no-op authorizer said %s", c.name(), show(got))
		}
	}
	if denied < 80 {
		t.Fatalf("only %d deny cells: the matrix lost rows", denied)
	}
}

// TestSweep checks every method of every service the frontend serves, for every kind of
// caller, against the rule stated in authorizer.go.
func TestSweep(t *testing.T) {
	var apis []string
	for _, desc := range []grpc.ServiceDesc{
		ws.WorkflowService_ServiceDesc, ops.OperatorService_ServiceDesc, adm.AdminService_ServiceDesc,
		healthpb.Health_ServiceDesc,
	} {
		for _, m := range desc.Methods {
			apis = append(apis, "/"+desc.ServiceName+"/"+m.MethodName)
		}
		for _, s := range desc.Streams {
			apis = append(apis, "/"+desc.ServiceName+"/"+s.StreamName)
		}
	}
	apis = append(apis, dispatchNexus, dispatchByEP, dispatchByTQ, completeNexus, completeChasm, reflection)

	callers := map[string]*authorization.Claims{
		"internal": internal, "none": anonymous, "acme:worker": acmeWorker, "acme:all": acmeAll,
		"acme:admin": acmeAdmin, "acme:write": acmeWrite, "acme:read": acmeRead,
		"temporal-system:admin": systemAdmin, "temporal-system:worker": systemWorker,
		"jwt sub=internal": fakeInternal, "buyer:worker+temporal-system:admin": buyerSysAdmin,
		"empty jwt": emptyJWT, "ACME:worker": upperWorker,
	}
	checked := 0
	for who, claims := range callers {
		for _, name := range apis {
			for _, ns := range []string{"acme", "buyer", "", "temporal-system"} {
				partyNS := ns != "" && ns != "temporal-system"
				hasWorker := claims != nil && claims.Namespaces[ns]&authorization.RoleWorker != 0
				want := deny
				switch {
				case who == "internal":
					want = allow
				case kontra.PublicAPIs[name]:
					want = allow
				case kontra.AllowedAPIs[name] && partyNS && hasWorker:
					want = allow
				}
				if got := decide(t, kontra.New(), claims, name, ns); got != want {
					t.Errorf("%s %s@%q: got %s, want %s", who, name, ns, show(got), show(want))
				}
				checked++
			}
		}
	}
	if len(apis) < 150 {
		t.Fatalf("only %d APIs swept", len(apis))
	}
	t.Logf("swept %d decisions over %d APIs", checked, len(apis))
}

// TestAllowedAPIsAreNamespaceScopedWorkflowCalls pins what the allowlist may contain: only
// WorkflowService calls whose request names a namespace, none of them administrative.
func TestAllowedAPIsAreNamespaceScopedWorkflowCalls(t *testing.T) {
	methods := ws.File_temporal_api_workflowservice_v1_service_proto.Services().
		ByName("WorkflowService").Methods()
	forbidden := map[string]bool{
		registerNS: true, updateNS: true, deprecateNS: true, listNS: true, batch: true,
		deleteWF: true, resetWF: true, schedule: true, versioning: true, deleteOp: true,
	}
	for name := range kontra.AllowedAPIs {
		md := api.GetMethodMetadata(name)
		if !strings.HasPrefix(name, api.WorkflowServicePrefix) || md.Scope != api.ScopeNamespace {
			t.Errorf("%s is not a namespace-scoped WorkflowService call", name)
		}
		if md.Access == api.AccessAdmin || forbidden[name] {
			t.Errorf("%s is administrative and must not be allowlisted", name)
		}
		m := methods.ByName(protoreflect.Name(api.MethodName(name)))
		if m == nil {
			t.Errorf("%s is not a WorkflowService method", name)
			continue
		}
		f := m.Input().Fields().ByName("namespace")
		if f == nil || f.Kind() != protoreflect.StringKind {
			t.Errorf("%s: request %s has no namespace field", name, m.Input().FullName())
		}
	}
	for name := range kontra.PublicAPIs {
		if kontra.AllowedAPIs[name] {
			t.Errorf("%s is in both sets", name)
		}
	}
}

// TestEveryListedAPIIsARealGRPCMethod: Temporal authorizes a call by its exact gRPC method
// name, so a listed name that is not one (a typo, a renamed or streaming method) would
// silently grant nothing; worse, a rename upstream would silently deny a worker. Every
// name must be a unary method of a service the frontend registers.
func TestEveryListedAPIIsARealGRPCMethod(t *testing.T) {
	unary := map[string]bool{}
	for _, desc := range []grpc.ServiceDesc{ws.WorkflowService_ServiceDesc, healthpb.Health_ServiceDesc} {
		for _, m := range desc.Methods {
			unary["/"+desc.ServiceName+"/"+m.MethodName] = true
		}
	}
	for set, names := range map[string]map[string]bool{"AllowedAPIs": kontra.AllowedAPIs, "PublicAPIs": kontra.PublicAPIs} {
		if len(names) == 0 {
			t.Errorf("%s is empty", set)
		}
		for name := range names {
			if !unary[name] {
				t.Errorf("%s: %q is not a unary method the frontend serves", set, name)
			}
		}
	}
}

func TestInternalPrincipalIsWhatTheInternalFrontendMaps(t *testing.T) {
	if !kontra.IsInternal(internal) {
		t.Fatal("the internal frontend's claims are not recognised as internal")
	}
	for _, c := range []*authorization.Claims{nil, fakeInternal, systemAdmin, acmeAll} {
		if kontra.IsInternal(c) {
			t.Fatalf("%+v must not be internal", c)
		}
	}
}

func TestDenyCarriesAReason(t *testing.T) {
	res, err := kontra.New().Authorize(context.Background(), buyerWorker,
		&authorization.CallTarget{APIName: describe, Namespace: "acme"})
	if err != nil || res.Decision != deny || !strings.HasPrefix(res.Reason, "kontra: ") {
		t.Fatalf("got %+v, %v", res, err)
	}
	res, _ = kontra.New().Authorize(context.Background(), acmeWorker,
		&authorization.CallTarget{APIName: describe, Namespace: "acme"})
	if res.Principal == nil || res.Principal.GetName() != "seller" || res.Principal.GetType() != "jwt" {
		t.Fatalf("an allowed call must carry the caller as principal, got %+v", res.Principal)
	}
}

// --- helpers ------------------------------------------------------------------------

func decide(t *testing.T, a authorization.Authorizer, claims *authorization.Claims, apiName, ns string) authorization.Decision {
	t.Helper()
	res, err := a.Authorize(context.Background(), claims, &authorization.CallTarget{APIName: apiName, Namespace: ns})
	if err != nil {
		t.Fatalf("authorize %s: %v", apiName, err)
	}
	return res.Decision
}

func show(d authorization.Decision) string {
	switch d {
	case allow:
		return "allow"
	case deny:
		return "deny"
	}
	return fmt.Sprintf("decision(%d)", d)
}

func internalClaims() *authorization.Claims {
	claims, err := authorization.NewInternalClaimMapper().GetClaims(&authorization.AuthInfo{})
	if err != nil {
		panic(err)
	}
	return claims
}

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

// mustJWT signs a token with these permissions and maps it with Temporal's default JWT
// claim mapper, exactly as the public frontend does.
func mustJWT(subject string, permissions ...string) *authorization.Claims {
	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": subject, "permissions": permissions, "exp": time.Now().Add(time.Hour).Unix(),
	})
	token.Header["kid"] = "test"
	signed, err := token.SignedString(signingKey)
	if err != nil {
		panic(err)
	}
	mapper := authorization.NewDefaultJWTClaimMapper(keys{}, &config.Authorization{}, log.NewNoopLogger())
	claims, err := mapper.GetClaims(&authorization.AuthInfo{AuthToken: "Bearer " + signed})
	if err != nil {
		panic(err)
	}
	return claims
}
