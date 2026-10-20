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
// does on the wire. Every request is the typed message Temporal would hand the authorizer
// (CallTarget.Request): a cell with no request of its own gets the one requestFor builds,
// naming the cell's namespace, job J and the stream handlers, the most favourable request a
// stream relay could send.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	commonpb "go.temporal.io/api/common/v1"
	ops "go.temporal.io/api/operatorservice/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	adm "go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	reflv1alpha "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

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
	healthList      = healthpb.Health_List_FullMethodName
	reflectionAlpha = reflv1alpha.ServerReflection_ServerReflectionInfo_FullMethodName
	updateWF        = ws.WorkflowService_UpdateWorkflowExecution_FullMethodName
	pollUpdate      = ws.WorkflowService_PollWorkflowExecutionUpdate_FullMethodName
	multiOp         = ws.WorkflowService_ExecuteMultiOperation_FullMethodName
	signalStart     = ws.WorkflowService_SignalWithStartWorkflowExecution_FullMethodName
	pauseWF         = ws.WorkflowService_PauseWorkflowExecution_FullMethodName
	wfOptions       = ws.WorkflowService_UpdateWorkflowExecutionOptions_FullMethodName
	buildIDCompat   = ws.WorkflowService_UpdateWorkerBuildIdCompatibility_FullMethodName
	stopBatch       = ws.WorkflowService_StopBatchOperation_FullMethodName
	// The worker controller's calls (ControllerAPIs) and the deployment calls it is denied.
	describeDeployment = ws.WorkflowService_DescribeWorkerDeployment_FullMethodName
	describeVersion    = ws.WorkflowService_DescribeWorkerDeploymentVersion_FullMethodName
	setCurrent         = ws.WorkflowService_SetWorkerDeploymentCurrentVersion_FullMethodName
	setRamping         = ws.WorkflowService_SetWorkerDeploymentRampingVersion_FullMethodName
	setManager         = ws.WorkflowService_SetWorkerDeploymentManager_FullMethodName
	versionMetadata    = ws.WorkflowService_UpdateWorkerDeploymentVersionMetadata_FullMethodName
	deleteVersion      = ws.WorkflowService_DeleteWorkerDeploymentVersion_FullMethodName
	deleteDeployment   = ws.WorkflowService_DeleteWorkerDeployment_FullMethodName
	listDeployments    = ws.WorkflowService_ListWorkerDeployments_FullMethodName
	createDeployment   = ws.WorkflowService_CreateWorkerDeployment_FullMethodName
	createVersion      = ws.WorkflowService_CreateWorkerDeploymentVersion_FullMethodName
	computeConfig      = ws.WorkflowService_UpdateWorkerDeploymentVersionComputeConfig_FullMethodName
	validateCompute    = ws.WorkflowService_ValidateWorkerDeploymentVersionComputeConfig_FullMethodName
	dispatchNexus      = nexusPrefix + "DispatchNexusTask"
	dispatchByEP       = nexusPrefix + "DispatchByEndpoint"
	dispatchByTQ       = nexusPrefix + "DispatchByNamespaceAndTaskQueue"
	completeNexus      = nexusPrefix + "CompleteNexusOperation"
	completeChasm      = nexusPrefix + "CompleteNexusOperationChasm"
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

	// The gate's stream relay for job J of acme: no permission at all, the subject
	// names.relay_subject builds. relayEmpty says "permissions": [] instead of leaving the
	// claim out; both map to no roles.
	relayJ     = mustJWT(kontra.RelaySubject("acme", jobJ))
	relayEmpty = mustJWTEmptyPermissions(kontra.RelaySubject("acme", jobJ))
	// The relay's subject with a role is a party token, judged by the party rule alone. An
	// unknown role still gives the token a namespace key (RoleUndefined).
	relayRead        = mustJWT(kontra.RelaySubject("acme", jobJ), "acme:read")
	relayUnknownRole = mustJWT(kontra.RelaySubject("acme", jobJ), "acme:relay")
	relayPlatform    = mustJWT(kontra.RelaySubject(kontra.PlatformNamespace, jobJ))
	relaySystem      = mustJWT(kontra.RelaySubject("temporal-system", jobJ))
	relayNoNS        = mustJWT(kontra.RelaySubject("", jobJ))
	// The worker controller (KON-15), and its subject on tokens that carry a role.
	controller       = mustJWT(kontra.ControllerSubject)
	controllerWorker = mustJWT(kontra.ControllerSubject, kontra.PlatformNamespace+":worker")
	controllerAdmin  = mustJWT(kontra.ControllerSubject, kontra.PlatformNamespace+":admin")
	// One of Kontra's own workers: a party token for the platform namespace (decisions I30 Q4).
	platformWorker = mustJWT("kontra-system-worker", kontra.PlatformNamespace+":worker")
)

// Job J is the relay's; job K is another job of the same seller.
const (
	jobJ = "job-0123456789abcdef0123456789abcdef"
	jobK = "job-fedcba9876543210fedcba9876543210"

	platform = kontra.PlatformNamespace
)

type cell struct {
	row    string // the §10.4 row (or decisions I3, I30 rule) this cell belongs to
	who    string
	claims *authorization.Claims
	api    string
	ns     string
	// The request Temporal hands the authorizer: nil for requestFor(api, ns, jobJ), the
	// favourable one; handshake for none at all, as for a stream.
	req any
	// Decisions of: Kontra's authorizer, Temporal's no-op authorizer, Temporal's default authorizer.
	kontra, noop, dflt authorization.Decision
}

// handshake stands for "no request": what a streaming RPC's authorization is given.
type streamHandshake struct{}

var handshake = streamHandshake{}

func (c cell) request() any {
	switch c.req {
	case nil:
		return requestFor(c.api, c.ns, jobJ)
	case handshake:
		return nil
	}
	return c.req
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
	{"worker-A", "acme:worker", acmeWorker, poll, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, pollActivity, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, pollNexus, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, respondNexus, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, respondWFT, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, start, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, signal, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, query, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describe, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "acme", nil, allow, allow, deny},
	// A seller's own backlog, for `temporal task-queue describe` and KEDA (issue #9).
	{"worker-A", "acme:worker", acmeWorker, describeTQ, "acme", nil, allow, allow, deny},
	{"worker-A", "acme:worker + every role", acmeAll, poll, "acme", nil, allow, allow, allow},
	// --- worker in namespace A: denied anything in B -----------------------------------
	{"worker-A", "acme:worker", acmeWorker, start, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describe, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, signal, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, poll, "other", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeOp, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "buyer", nil, deny, allow, deny},
	// Allowed in acme, refused in every other namespace: the backlog is the token's own.
	{"worker-A", "acme:worker", acmeWorker, describeTQ, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeTQ, "other", nil, deny, allow, deny},
	// --- worker in namespace A: denied all Operator and Admin APIs ---------------------
	{"worker-A", "acme:worker", acmeWorker, listClusters, "", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, addSearchAttr, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteNS, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeCluster, "", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, replication, "", nil, deny, allow, deny},
	// --- worker in namespace A: denied the Nexus HTTP API (DispatchNexusTask) ----------
	{"worker-A", "acme:worker", acmeWorker, dispatchNexus, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, dispatchByEP, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, dispatchByTQ, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, completeNexus, "buyer", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, completeChasm, "buyer", nil, deny, allow, deny},
	// --- worker in namespace A: denied namespace administration and unneeded APIs ------
	{"worker-A", "acme:worker", acmeWorker, updateNS, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deprecateNS, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, registerNS, "acme2", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, batch, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteWF, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, resetWF, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, schedule, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, versioning, "acme", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, deleteOp, "acme", nil, deny, allow, deny},
	// --- worker in namespace A: denied calls that name no namespace --------------------
	{"worker-A", "acme:worker", acmeWorker, listNS, "", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, searchAttrs, "", nil, deny, allow, deny},
	{"worker-A", "acme:worker", acmeWorker, describeNS, "", nil, deny, allow, deny}, // DescribeNamespace by id
	{"worker-A", "acme:worker", acmeWorker, start, "", nil, deny, allow, deny},
	// --- namespace keys are compared exactly -------------------------------------------
	{"exact-namespace", "acme:worker", acmeWorker, start, "ACME", nil, deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme ", nil, deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme\n", nil, deny, allow, deny},
	{"exact-namespace", "acme:worker", acmeWorker, start, "\u0430cme", nil, deny, allow, deny},                // Cyrillic a
	{"exact-namespace", "acme:worker", acmeWorker, start, "acme\u200b", nil, deny, allow, deny},               // zero-width space
	{"exact-namespace", "acme:worker", acmeWorker, start, "\uff41\uff43\uff4d\uff45", nil, deny, allow, deny}, // full width
	{"exact-namespace", "ACME:worker", upperWorker, start, "acme", nil, deny, allow, deny},
	{"exact-namespace", "acme:WORKER (roles are case-insensitive in Temporal)", acmeWORKER, start, "acme", nil, allow, allow, deny},
	{"exact-namespace", "temporal-system worker", sysNSWorker, start, "temporal-system", nil, deny, allow, deny},

	// --- any token: no Nexus endpoint API (the publisher role is gone, decisions I3) ---
	{"endpoints", "acme:worker", acmeWorker, createEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, updateEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, deleteEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, getEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:worker", acmeWorker, listEndpoints, "", nil, deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, createEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, updateEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, deleteEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "acme:admin", acmeAdmin, listEndpoints, "", nil, deny, allow, deny},
	{"endpoints", "temporal-system:admin", systemAdmin, createEndpoint, "", nil, deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, updateEndpoint, "", nil, deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, deleteEndpoint, "", nil, deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, getEndpoint, "", nil, deny, allow, allow},
	{"endpoints", "temporal-system:admin", systemAdmin, listEndpoints, "", nil, deny, allow, allow},
	{"endpoints", "buyer:worker", buyerWorker, createEndpoint, "", nil, deny, allow, deny},
	{"endpoints", "buyer:worker", buyerWorker, listEndpoints, "", nil, deny, allow, deny},

	// --- buyer in namespace C: allowed to start workflows and call endpoints in C -------
	{"buyer-C", "buyer:worker", buyerWorker, start, "buyer", nil, allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, startOp, "buyer", nil, allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeOp, "buyer", nil, allow, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollOp, "buyer", nil, allow, allow, deny},
	// --- buyer in namespace C: denied describing or listing anything in A ---------------
	{"buyer-C", "buyer:worker", buyerWorker, describe, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, query, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, history, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, list, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, count, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, signal, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, cancel, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, terminate, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeOp, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, describeNS, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, startOp, "acme", nil, deny, allow, deny},
	// --- a worker of C cannot poll A's task queues --------------------------------------
	{"buyer-C", "buyer:worker", buyerWorker, poll, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollActivity, "acme", nil, deny, allow, deny},
	{"buyer-C", "buyer:worker", buyerWorker, pollNexus, "acme", nil, deny, allow, deny},
	{"buyer-C", "other:worker", otherWorker, pollNexus, "acme", nil, deny, allow, deny},
	// --- extra roles in a buyer token grant nothing in A (the default authorizer leaks) ---
	{"buyer-C", "buyer:worker+temporal-system:read", buyerSysRead, describe, "acme", nil, deny, allow, allow},
	{"buyer-C", "buyer:worker+temporal-system:read", buyerSysRead, list, "acme", nil, deny, allow, allow},
	{"buyer-C", "buyer:worker+temporal-system:admin", buyerSysAdmin, pollNexus, "acme", nil, deny, allow, allow},
	{"buyer-C", "buyer:worker+acme:read", buyerAcmeRead, describe, "acme", nil, deny, allow, allow},

	// --- control plane: everything, but only as the internal principal -----------------
	{"control-plane", "internal", internal, createEndpoint, "", nil, allow, allow, allow},
	{"control-plane", "internal", internal, listEndpoints, "", nil, allow, allow, allow},
	{"control-plane", "internal", internal, registerNS, "newparty", nil, allow, allow, allow},
	{"control-plane", "internal", internal, updateNS, "acme", nil, allow, allow, allow},
	{"control-plane", "internal", internal, describe, "acme", nil, allow, allow, allow},
	{"control-plane", "internal", internal, describeCluster, "", nil, allow, allow, allow},
	{"control-plane", "internal", internal, replication, "", nil, allow, allow, allow},
	{"control-plane", "internal", internal, dispatchByEP, "acme", nil, allow, allow, allow}, // history's Nexus dispatch
	{"control-plane", "internal", internal, completeNexus, "buyer", nil, allow, allow, allow},
	// --- a JWT is never the control plane, whatever its roles or subject ----------------
	{"control-plane", "temporal-system:admin", systemAdmin, listClusters, "", nil, deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, describeCluster, "", nil, deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, registerNS, "newparty", nil, deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, deleteNS, "acme", nil, deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, start, "acme", nil, deny, allow, allow},
	{"control-plane", "temporal-system:admin", systemAdmin, dispatchByEP, "acme", nil, deny, allow, allow},
	{"control-plane", "temporal-system:worker", systemWorker, poll, "acme", nil, deny, allow, deny},
	{"control-plane", "jwt sub=internal", fakeInternal, createEndpoint, "", nil, deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, updateNS, "acme", nil, deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, deleteNS, "acme", nil, deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, addSearchAttr, "acme", nil, deny, allow, allow},
	{"control-plane", "acme:admin", acmeAdmin, listClusters, "", nil, deny, allow, deny},

	// --- roles other than worker grant nothing, even in their own namespace ------------
	{"other-roles", "acme:admin", acmeAdmin, poll, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:admin", acmeAdmin, start, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:write", acmeWrite, start, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:write", acmeWrite, batch, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:read", acmeRead, describe, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:worker+every role", acmeAll, batch, "acme", nil, deny, allow, allow},
	{"other-roles", "acme:worker+every role", acmeAll, updateNS, "acme", nil, deny, allow, allow},

	// --- no token: health checks and system info only ----------------------------------
	{"no-token", "none", anonymous, healthCheck, "", nil, allow, allow, allow},
	{"no-token", "none", anonymous, systemInfo, "", nil, allow, allow, allow},
	{"no-token", "none", anonymous, clusterInfo, "", nil, allow, allow, deny},
	{"no-token", "none", anonymous, start, "acme", nil, deny, allow, deny},
	{"no-token", "none", anonymous, describeNS, "acme", nil, deny, allow, deny},
	{"no-token", "none", anonymous, listEndpoints, "", nil, deny, allow, deny},
	{"no-token", "none", anonymous, describeCluster, "", nil, deny, allow, deny},
	{"no-token", "none", anonymous, dispatchByEP, "acme", nil, deny, allow, deny},
	{"no-token", "none", anonymous, completeNexus, "buyer", nil, deny, allow, deny},
	{"no-token", "none", anonymous, healthWatch, "", nil, deny, allow, deny},
	{"no-token", "none", anonymous, reflection, "", nil, deny, allow, deny},
	{"any-token", "acme:worker", acmeWorker, systemInfo, "", nil, allow, allow, allow},
	{"any-token", "acme:worker", acmeWorker, clusterInfo, "", nil, allow, allow, deny},
	{"any-token", "acme:worker", acmeWorker, healthCheck, "", nil, allow, allow, allow},
	{"any-token", "acme:worker", acmeWorker, reflection, "", nil, deny, allow, deny},
	// Both reflection streams are registered (grpc 1.84 Register), and Health/List is new.
	{"no-token", "none", anonymous, reflectionAlpha, "", nil, deny, allow, deny},
	{"no-token", "none", anonymous, healthList, "", nil, deny, allow, deny},
	{"any-token", "acme:worker", acmeWorker, reflectionAlpha, "", nil, deny, allow, deny},
	{"any-token", "acme:worker", acmeWorker, healthList, "", nil, deny, allow, deny},

	// --- party token: its own workflows, steered (seller self-access, decisions I30) ---
	// The seller's own subscriber and publisher, harness approvals from the dev console's
	// session pane (#54), stock temporalio update handles: escrow reads none of it.
	{"seller-self", "acme:worker", acmeWorker, updateWF, "acme", upd("acme", jobJ, "approve"), allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, updateWF, "acme", nil, allow, allow, deny}, // its own stream's poll
	{"seller-self", "acme:worker", acmeWorker, query, "acme", nil, allow, allow, deny},    // its own stream's offset
	{"seller-self", "acme:worker", acmeWorker, pollUpdate, "acme", nil, allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, signal, "acme", sig("acme", jobJ, streamPublish), allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, signalStart, "acme", nil, allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, multiOp, "acme", updateWithStart("acme", jobJ, "approve"), allow, allow, deny},
	// Terminating or canceling its own job is a refund counted against the seller.
	{"seller-self", "acme:worker", acmeWorker, terminate, "acme", nil, allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, cancel, "acme", nil, allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, history, "acme", nil, allow, allow, deny},
	{"seller-self", "acme:worker", acmeWorker, list, "acme", nil, allow, allow, deny},

	// --- party token: the denials escrow depends on, in a seller's and a buyer's namespace
	// A reset brings a terminated pinned run back to life in the same chain, completion
	// callbacks included: completed after the buyer's cancel, it is a delivery (breaks I17).
	{"escrow-guards", "acme:worker", acmeWorker, resetWF, "acme", nil, deny, allow, deny},
	// Deleting the I16 placeholder reopens cancel-then-start; deleting history erases what
	// a dispute is argued from.
	{"escrow-guards", "acme:worker", acmeWorker, deleteWF, "acme", nil, deny, allow, deny},
	// The same provenance griefing as the buyer's below, through a standalone operation.
	{"escrow-guards", "acme:worker", acmeWorker, deleteOp, "acme", nil, deny, allow, deny},
	// Batch jobs run as Temporal's internal principal, skipping this policy and the callback
	// guard: a batch reset is the reset above.
	{"escrow-guards", "acme:worker", acmeWorker, batch, "acme", nil, deny, allow, deny},
	{"escrow-guards", "acme:worker", acmeWorker, stopBatch, "acme", nil, deny, allow, deny},
	// A schedule's actions run in the scheduler as the internal principal, and a schedule
	// outlives a revoked session.
	{"escrow-guards", "acme:worker", acmeWorker, schedule, "acme", nil, deny, allow, deny},
	// Retention decides how long the records escrow reads exist.
	{"escrow-guards", "acme:worker", acmeWorker, updateNS, "acme", nil, deny, allow, deny},
	// The escrow observer has not been measured against paused runs.
	{"escrow-guards", "acme:worker", acmeWorker, pauseWF, "acme", nil, deny, allow, deny},
	// Versioning overrides on a pinned run; no consumer.
	{"escrow-guards", "acme:worker", acmeWorker, wfOptions, "acme", nil, deny, allow, deny},
	// Seller Worker Versioning stays denied in v1 (I30, Q5): a seller that turns on
	// use_worker_versioning gets no tasks.
	{"escrow-guards", "acme:worker", acmeWorker, setCurrent, "acme", nil, deny, allow, deny},
	{"escrow-guards", "acme:worker", acmeWorker, buildIDCompat, "acme", nil, deny, allow, deny},
	// Compute config makes the server provision and invoke workers on a cloud provider,
	// with the server's own identity.
	{"escrow-guards", "acme:worker", acmeWorker, createVersion, "acme", nil, deny, allow, deny},
	{"escrow-guards", "acme:worker", acmeWorker, computeConfig, "acme", nil, deny, allow, deny},
	{"escrow-guards", "acme:worker", acmeWorker, validateCompute, "acme", nil, deny, allow, deny},
	// A buyer's records are where provenance is read: a missing record is UNVERIFIED, never
	// a delivery, so deleting its caller workflow or operation, resetting it or shortening
	// its retention would turn a real delivery into a refund counted against the seller.
	{"escrow-guards", "buyer:worker", buyerWorker, deleteWF, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, deleteOp, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, resetWF, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, updateNS, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, batch, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, schedule, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, pauseWF, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, wfOptions, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, setCurrent, "buyer", nil, deny, allow, deny},
	{"escrow-guards", "buyer:worker", buyerWorker, computeConfig, "buyer", nil, deny, allow, deny},

	// --- party tokens in the platform namespace: nothing ------------------------------
	{"platform-ns", "acme:worker", acmeWorker, poll, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, start, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, signal, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, updateWF, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, query, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, describe, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, list, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, describeNS, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, describeTQ, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, setCurrent, platform, nil, deny, allow, deny},
	{"platform-ns", "acme:worker", acmeWorker, describeDeployment, platform, nil, deny, allow, deny},
	{"platform-ns", "buyer:worker", buyerWorker, poll, platform, nil, deny, allow, deny},
	{"platform-ns", "buyer:worker", buyerWorker, start, platform, nil, deny, allow, deny},
	{"platform-ns", "buyer:worker", buyerWorker, startOp, platform, nil, deny, allow, deny},
	{"platform-ns", "buyer:worker", buyerWorker, describe, platform, nil, deny, allow, deny},
	{"platform-ns", "buyer:worker", buyerWorker, count, platform, nil, deny, allow, deny},
	// Kontra's own workers: the party rule, in the platform namespace only; no deployment call.
	{"platform-ns", "kontra-system:worker", platformWorker, poll, platform, nil, allow, allow, deny},
	{"platform-ns", "kontra-system:worker", platformWorker, pollActivity, platform, nil, allow, allow, deny},
	{"platform-ns", "kontra-system:worker", platformWorker, start, "acme", nil, deny, allow, deny},
	{"platform-ns", "kontra-system:worker", platformWorker, setCurrent, platform, nil, deny, allow, deny},

	// --- the stream relay: its job's poll Update and offset Query, nothing else ---------
	// The default authorizer denies a token with no role, so it cannot express this either.
	{"relay-J", "relay(acme,J)", relayJ, updateWF, "acme", nil, allow, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, query, "acme", nil, allow, allow, deny},
	{"relay-J", "relay(acme,J) permissions:[]", relayEmpty, updateWF, "acme", nil, allow, allow, deny},
	{"relay-J", "relay(acme,J) permissions:[]", relayEmpty, query, "acme", nil, allow, allow, deny},
	// Another job of the same seller, and job J's id in another namespace.
	{"relay-J", "relay(acme,J) job K", relayJ, updateWF, "acme", upd("acme", jobK, kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) job K", relayJ, query, "acme", qry("acme", jobK, kontra.StreamOffsetQuery), deny, allow, deny},
	{"relay-J", "relay(acme,J) job J@other", relayJ, updateWF, "other", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J) job J@other", relayJ, query, "other", nil, deny, allow, deny},
	// Any other handler: the harness's approvals, the publish signal's name, none, the
	// built-in queries that expose a workflow's internals, the two names swapped.
	{"relay-J", "relay(acme,J) update approve", relayJ, updateWF, "acme", upd("acme", jobJ, "approve"), deny, allow, deny},
	{"relay-J", "relay(acme,J) update publish", relayJ, updateWF, "acme", upd("acme", jobJ, streamPublish), deny, allow, deny},
	{"relay-J", "relay(acme,J) update unnamed", relayJ, updateWF, "acme", upd("acme", jobJ, ""), deny, allow, deny},
	{"relay-J", "relay(acme,J) update offset name", relayJ, updateWF, "acme", upd("acme", jobJ, kontra.StreamOffsetQuery), deny, allow, deny},
	{"relay-J", "relay(acme,J) query __stack_trace", relayJ, query, "acme", qry("acme", jobJ, "__stack_trace"), deny, allow, deny},
	{"relay-J", "relay(acme,J) query metadata", relayJ, query, "acme", qry("acme", jobJ, "__temporal_workflow_metadata"), deny, allow, deny},
	{"relay-J", "relay(acme,J) query poll name", relayJ, query, "acme", qry("acme", jobJ, kontra.StreamPollUpdate), deny, allow, deny},
	// A request's type must be its API's: Temporal itself hands an API name with another
	// request type (cross-namespace command checks pass the workflow task's response).
	{"relay-J", "relay(acme,J) update under Query", relayJ, query, "acme", upd("acme", jobJ, kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) query under Update", relayJ, updateWF, "acme", qry("acme", jobJ, kontra.StreamOffsetQuery), deny, allow, deny},
	{"relay-J", "relay(acme,J) query under Signal", relayJ, signal, "acme", qry("acme", jobJ, kontra.StreamOffsetQuery), deny, allow, deny},
	// Update-with-start embeds an Update; the publish signal would inject events.
	{"relay-J", "relay(acme,J)", relayJ, multiOp, "acme", updateWithStart("acme", jobJ, kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) publish", relayJ, signal, "acme", sig("acme", jobJ, streamPublish), deny, allow, deny},
	// PollWorkflowExecutionUpdate names no update, so it would read any update by id; the
	// strict relay re-sends its Update instead (decisions I30, Q1).
	{"relay-J", "relay(acme,J)", relayJ, pollUpdate, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, describe, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, history, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, terminate, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, cancel, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, start, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, signalStart, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, list, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, count, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, describeTQ, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, poll, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, describeNS, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, startOp, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, listEndpoints, "", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, describeCluster, "", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, dispatchByEP, "acme", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J) stream handshake", relayJ, updateWF, "acme", handshake, deny, allow, deny},
	{"relay-J", "relay(acme,J) stream handshake", relayJ, reflection, "", nil, deny, allow, deny},
	// Never in the platform namespace, Temporal's own or none.
	{"relay-J", "relay(acme,J)", relayJ, updateWF, platform, nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, updateWF, "temporal-system", nil, deny, allow, deny},
	{"relay-J", "relay(acme,J)", relayJ, updateWF, "", nil, deny, allow, deny},
	// Requests that might rebuild the same subject from other parts: the namespace
	// swallowing the separator, the workflow id carrying it, another split, a trailing
	// newline, upper case.
	{"relay-J", "relay(acme,J) ns swallows /", relayJ, updateWF, "acme/" + jobJ, upd("acme/"+jobJ, "", kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) wf carries /", relayJ, updateWF, "acme", upd("acme", "/"+jobJ, kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) other split", relayJ, updateWF, "acm", upd("acm", "e/"+jobJ, kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) trailing newline", relayJ, updateWF, "acme", upd("acme", jobJ+"\n", kontra.StreamPollUpdate), deny, allow, deny},
	{"relay-J", "relay(acme,J) upper case", relayJ, updateWF, "acme", upd("acme", strings.ToUpper(jobJ), kontra.StreamPollUpdate), deny, allow, deny},
	// A token with any permission is a party token, whatever its subject: the relay's
	// subject with a role gets only what that role gets here (nothing, but worker).
	{"relay-J", "relay subject + acme:read", relayRead, updateWF, "acme", nil, deny, allow, deny},
	{"relay-J", "relay subject + acme:read", relayRead, query, "acme", nil, deny, allow, allow},
	{"relay-J", "relay subject + acme:relay", relayUnknownRole, updateWF, "acme", nil, deny, allow, deny},
	{"relay-J", "relay subject + acme:relay", relayUnknownRole, query, "acme", nil, deny, allow, deny},
	// Relay subjects the gate never mints (relay_subject refuses a reserved or empty
	// namespace) are refused here too: a relay acts in a party's namespace only.
	{"relay-J", "relay(kontra-system,J)", relayPlatform, updateWF, platform, nil, deny, allow, deny},
	{"relay-J", "relay(temporal-system,J)", relaySystem, updateWF, "temporal-system", nil, deny, allow, deny},
	{"relay-J", "relay(\"\",J)", relayNoNS, updateWF, "", nil, deny, allow, deny},
	// A token with no subject is nobody's relay.
	{"relay-J", "empty jwt", emptyJWT, updateWF, "acme", nil, deny, allow, deny},
	{"relay-J", "empty jwt", emptyJWT, query, "acme", nil, deny, allow, deny},

	// --- the worker controller: deployment calls, in the platform namespace only --------
	{"controller", "controller", controller, describeDeployment, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, describeVersion, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, setCurrent, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, setRamping, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, setManager, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, versionMetadata, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, deleteVersion, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, deleteDeployment, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, describeTQ, platform, nil, allow, allow, deny},
	{"controller", "controller", controller, count, platform, nil, allow, allow, deny},
	// The same ten calls in a seller's namespace.
	{"controller", "controller", controller, describeDeployment, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, describeVersion, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, setCurrent, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, setRamping, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, setManager, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, versionMetadata, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, deleteVersion, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, deleteDeployment, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, describeTQ, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, count, "acme", nil, deny, allow, deny},
	{"controller", "controller", controller, setCurrent, "buyer", nil, deny, allow, deny},
	{"controller", "controller", controller, setCurrent, "", nil, deny, allow, deny},
	{"controller", "controller", controller, setCurrent, "temporal-system", nil, deny, allow, deny},
	// In the platform namespace, nothing but those ten: no task, no workflow (a stolen
	// credential would start any platform workflow with any input), no rollout gate
	// workflows (I30, Q3), no compute config, no versioning rules.
	{"controller", "controller", controller, poll, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, pollActivity, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, start, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, describe, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, signal, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, updateWF, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, query, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, terminate, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, resetWF, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, deleteWF, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, batch, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, schedule, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, listDeployments, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, createDeployment, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, createVersion, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, computeConfig, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, validateCompute, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, versioning, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, buildIDCompat, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, wfOptions, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, describeNS, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, updateNS, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, listClusters, "", nil, deny, allow, deny},
	{"controller", "controller", controller, describeCluster, "", nil, deny, allow, deny},
	{"controller", "controller", controller, addSearchAttr, platform, nil, deny, allow, deny},
	{"controller", "controller", controller, dispatchByEP, platform, nil, deny, allow, deny},
	// Its subject with a role is a party token: a platform worker, never a controller.
	{"controller", "controller + kontra-system:worker", controllerWorker, setCurrent, platform, nil, deny, allow, deny},
	{"controller", "controller + kontra-system:worker", controllerWorker, describeDeployment, platform, nil, deny, allow, deny},
	{"controller", "controller + kontra-system:worker", controllerWorker, poll, platform, nil, allow, allow, deny},
	{"controller", "controller + kontra-system:admin", controllerAdmin, setCurrent, platform, nil, deny, allow, allow},
	{"controller", "controller + kontra-system:admin", controllerAdmin, describeDeployment, platform, nil, deny, allow, allow},
	// A party token on the deployment calls, anywhere.
	{"controller", "acme:worker", acmeWorker, describeDeployment, "acme", nil, deny, allow, deny},
	{"controller", "acme:worker", acmeWorker, setRamping, "acme", nil, deny, allow, deny},
	{"controller", "acme:worker", acmeWorker, setRamping, platform, nil, deny, allow, deny},
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
				got := decide(t, az.a, c.claims, c.api, c.ns, c.request())
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
		if got := decide(t, authorization.NewNoopAuthorizer(), c.claims, c.api, c.ns, c.request()); got != allow {
			t.Errorf("%s: the no-op authorizer said %s", c.name(), show(got))
		}
	}
	if denied < 230 {
		t.Fatalf("only %d deny cells: the matrix lost rows", denied)
	}
	t.Logf("%d deny cells, %d allow cells", denied, len(matrix)-denied)
}

// sweepCaller is a caller of TestSweep, with what it is in the policy's terms, stated
// here from how its token was made rather than read from the claims.
type sweepCaller struct {
	who    string
	claims *authorization.Claims
	kind   string // internal, anonymous, party (a token with a permission), controller, relay, nobody
	// For a relay: the namespace and workflow its subject was built from.
	relayNS, relayWF string
}

// sweepRequest is one of the requests TestSweep sends to every API: the workflow it names
// and the handler names an Update or a Query carries, or none at all.
type sweepRequest struct {
	label, wf, update, query string
	none                     bool
}

// TestSweep checks every method of every service the frontend serves, for every kind of
// caller, every kind of namespace and both a favourable and hostile requests, against the
// rule stated in authorizer.go, written again here independently of its code.
func TestSweep(t *testing.T) {
	apis := frontendAPIs()
	callers := []sweepCaller{
		{"internal", internal, "internal", "", ""},
		{"none", anonymous, "anonymous", "", ""},
		{"acme:worker", acmeWorker, "party", "", ""},
		{"acme:all", acmeAll, "party", "", ""},
		{"acme:admin", acmeAdmin, "party", "", ""},
		{"acme:write", acmeWrite, "party", "", ""},
		{"acme:read", acmeRead, "party", "", ""},
		{"temporal-system:admin", systemAdmin, "party", "", ""},
		{"temporal-system:worker", systemWorker, "party", "", ""},
		{"jwt sub=internal", fakeInternal, "party", "", ""},
		{"buyer:worker+temporal-system:admin", buyerSysAdmin, "party", "", ""},
		{"ACME:worker", upperWorker, "party", "", ""},
		{"kontra-system:worker", platformWorker, "party", "", ""},
		{"relay subject + acme:read", relayRead, "party", "", ""},
		{"relay subject + acme:relay", relayUnknownRole, "party", "", ""},
		{"controller + kontra-system:worker", controllerWorker, "party", "", ""},
		{"controller + kontra-system:admin", controllerAdmin, "party", "", ""},
		{"controller", controller, "controller", "", ""},
		{"relay(acme,J)", relayJ, "relay", "acme", jobJ},
		{"relay(acme,J) permissions:[]", relayEmpty, "relay", "acme", jobJ},
		{"empty jwt", emptyJWT, "nobody", "", ""},
	}
	namespaces := []string{"acme", "buyer", "", "temporal-system", "kontra-system"}
	requests := []sweepRequest{
		{"favourable", jobJ, "__temporal_workflow_stream_poll", "__temporal_workflow_stream_offset", false},
		{"job K", jobK, "__temporal_workflow_stream_poll", "__temporal_workflow_stream_offset", false},
		{"other handlers", jobJ, "approve", "__stack_trace", false},
		{"no request", "", "", "", true},
	}
	built := map[string]any{}
	requestOf := func(name, ns string, r sweepRequest) any {
		if r.none {
			return nil
		}
		key := name + "\x00" + ns + "\x00" + r.label
		if req, ok := built[key]; !ok {
			built[key] = requestWith(name, ns, r.wf, r.update, r.query)
		} else {
			return req
		}
		return built[key]
	}
	checked := 0
	for _, c := range callers {
		for _, name := range apis {
			for _, ns := range namespaces {
				for _, r := range requests {
					want := deny
					switch {
					case c.kind == "internal":
						want = allow
					case kontra.PublicAPIs[name]:
						want = allow
					case c.kind == "party":
						if kontra.AllowedAPIs[name] && ns != "" && ns != "temporal-system" &&
							c.claims.Namespaces[ns]&authorization.RoleWorker != 0 {
							want = allow
						}
					case c.kind == "controller":
						if kontra.ControllerAPIs[name] && ns == "kontra-system" {
							want = allow
						}
					case c.kind == "relay":
						handler := (name == "/temporal.api.workflowservice.v1.WorkflowService/UpdateWorkflowExecution" &&
							r.update == "__temporal_workflow_stream_poll") ||
							(name == "/temporal.api.workflowservice.v1.WorkflowService/QueryWorkflow" &&
								r.query == "__temporal_workflow_stream_offset")
						if handler && !r.none && ns == c.relayNS && r.wf == c.relayWF {
							want = allow
						}
					}
					if got := decide(t, kontra.New(), c.claims, name, ns, requestOf(name, ns, r)); got != want {
						t.Errorf("%s %s@%q (%s): got %s, want %s", c.who, name, ns, r.label, show(got), show(want))
					}
					checked++
				}
			}
		}
	}
	if len(apis) < 150 {
		t.Fatalf("only %d APIs swept", len(apis))
	}
	t.Logf("swept %d decisions over %d APIs", checked, len(apis))
}

// frontendAPIs is every name the frontend hands the authorizer: every method and stream of
// every service it registers (both reflection versions included), and the Nexus HTTP API's
// pseudo-methods.
func frontendAPIs() []string {
	var apis []string
	for _, desc := range []grpc.ServiceDesc{
		ws.WorkflowService_ServiceDesc, ops.OperatorService_ServiceDesc, adm.AdminService_ServiceDesc,
		healthpb.Health_ServiceDesc, reflectionpb.ServerReflection_ServiceDesc,
		reflv1alpha.ServerReflection_ServiceDesc,
	} {
		for _, m := range desc.Methods {
			apis = append(apis, "/"+desc.ServiceName+"/"+m.MethodName)
		}
		for _, s := range desc.Streams {
			apis = append(apis, "/"+desc.ServiceName+"/"+s.StreamName)
		}
	}
	return append(apis, dispatchNexus, dispatchByEP, dispatchByTQ, completeNexus, completeChasm)
}

// neverAllowed are calls no token may ever make, whatever the principal: each one would
// let a party or a stolen platform credential change what escrow observes, act as
// Temporal's internal principal, or make the server act on a cloud provider (decisions I30).
var neverAllowed = map[string]bool{
	resetWF:   true,
	deleteWF:  true,
	deleteOp:  true,
	batch:     true,
	stopBatch: true,
	ws.WorkflowService_DescribeBatchOperation_FullMethodName:    true,
	ws.WorkflowService_ListBatchOperations_FullMethodName:       true,
	ws.WorkflowService_CreateSchedule_FullMethodName:            true,
	ws.WorkflowService_DescribeSchedule_FullMethodName:          true,
	ws.WorkflowService_UpdateSchedule_FullMethodName:            true,
	ws.WorkflowService_PatchSchedule_FullMethodName:             true,
	ws.WorkflowService_ListScheduleMatchingTimes_FullMethodName: true,
	ws.WorkflowService_DeleteSchedule_FullMethodName:            true,
	ws.WorkflowService_ListSchedules_FullMethodName:             true,
	ws.WorkflowService_CountSchedules_FullMethodName:            true,
	registerNS:      true,
	updateNS:        true,
	deprecateNS:     true,
	createVersion:   true,
	computeConfig:   true,
	validateCompute: true,
}

// TestAllowedAPIsAreNamespaceScopedWorkflowCalls pins what the allowlists may contain: only
// WorkflowService calls whose request names a namespace, none of them administrative, and
// none of the calls no token may ever make.
func TestAllowedAPIsAreNamespaceScopedWorkflowCalls(t *testing.T) {
	methods := ws.File_temporal_api_workflowservice_v1_service_proto.Services().
		ByName("WorkflowService").Methods()
	forbidden := map[string]bool{listNS: true, versioning: true, buildIDCompat: true, wfOptions: true, pauseWF: true}
	for set, names := range map[string]map[string]bool{
		"AllowedAPIs": kontra.AllowedAPIs, "ControllerAPIs": kontra.ControllerAPIs, "RelayAPIs": kontra.RelayAPIs,
	} {
		for name := range names {
			md := api.GetMethodMetadata(name)
			if !strings.HasPrefix(name, api.WorkflowServicePrefix) || md.Scope != api.ScopeNamespace {
				t.Errorf("%s: %s is not a namespace-scoped WorkflowService call", set, name)
			}
			if md.Access == api.AccessAdmin || forbidden[name] || neverAllowed[name] {
				t.Errorf("%s: %s must not be allowlisted", set, name)
			}
			m := methods.ByName(protoreflect.Name(api.MethodName(name)))
			if m == nil {
				t.Errorf("%s: %s is not a WorkflowService method", set, name)
				continue
			}
			f := m.Input().Fields().ByName("namespace")
			if f == nil || f.Kind() != protoreflect.StringKind {
				t.Errorf("%s: %s: request %s has no namespace field", set, name, m.Input().FullName())
			}
			if kontra.PublicAPIs[name] {
				t.Errorf("%s: %s is public too", set, name)
			}
		}
	}
	for name := range neverAllowed {
		if kontra.PublicAPIs[name] {
			t.Errorf("%s must never be allowed, and is public", name)
		}
	}
	// The controller's and the relay's sets are exactly the calls I30 decided, nothing more.
	want := map[string]map[string]bool{
		"ControllerAPIs": {describeDeployment: true, describeVersion: true, setCurrent: true, setRamping: true,
			setManager: true, versionMetadata: true, deleteVersion: true, deleteDeployment: true,
			describeTQ: true, count: true},
		"RelayAPIs": {updateWF: true, query: true},
	}
	for set, got := range map[string]map[string]bool{"ControllerAPIs": kontra.ControllerAPIs, "RelayAPIs": kontra.RelayAPIs} {
		if len(got) != len(want[set]) {
			t.Errorf("%s has %d entries, want %d", set, len(got), len(want[set]))
		}
		for name := range got {
			if !want[set][name] {
				t.Errorf("%s: %s was not decided (decisions I30)", set, name)
			}
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
	for set, names := range map[string]map[string]bool{
		"AllowedAPIs": kontra.AllowedAPIs, "PublicAPIs": kontra.PublicAPIs,
		"ControllerAPIs": kontra.ControllerAPIs, "RelayAPIs": kontra.RelayAPIs,
	} {
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
	for _, c := range []*authorization.Claims{nil, fakeInternal, systemAdmin, acmeAll, relayJ, controller, emptyJWT} {
		if kontra.IsInternal(c) {
			t.Fatalf("%+v must not be internal", c)
		}
	}
}

func TestDenyCarriesAReason(t *testing.T) {
	for _, c := range []struct {
		claims *authorization.Claims
		api    string
		ns     string
		req    any
	}{
		{buyerWorker, describe, "acme", nil},
		{relayJ, updateWF, "acme", upd("acme", jobK, kontra.StreamPollUpdate)},
		{relayJ, describe, "acme", requestFor(describe, "acme", jobJ)},
		{controller, setCurrent, "acme", nil},
		{controller, start, platform, nil},
	} {
		res, err := kontra.New().Authorize(context.Background(), c.claims,
			&authorization.CallTarget{APIName: c.api, Namespace: c.ns, Request: c.req})
		if err != nil || res.Decision != deny || !strings.HasPrefix(res.Reason, "kontra: ") {
			t.Fatalf("%s@%s: got %+v, %v", c.api, c.ns, res, err)
		}
	}
	for who, c := range map[string]struct {
		claims *authorization.Claims
		api    string
		ns     string
	}{
		"seller":                          {acmeWorker, describe, "acme"},
		kontra.RelaySubject("acme", jobJ): {relayJ, query, "acme"},
		kontra.ControllerSubject:          {controller, describeDeployment, platform},
	} {
		res, _ := kontra.New().Authorize(context.Background(), c.claims,
			&authorization.CallTarget{APIName: c.api, Namespace: c.ns, Request: requestFor(c.api, c.ns, jobJ)})
		if res.Decision != allow || res.Principal == nil || res.Principal.GetName() != who || res.Principal.GetType() != "jwt" {
			t.Fatalf("an allowed call must carry the caller as principal, got %+v", res)
		}
	}
}

// golden is temporal/authorizer/testdata/principals.json, the one file both languages
// read: tests/contract/test_names.py checks kontra.contract.names against the same vectors,
// so the two constructions of a relay subject cannot drift apart.
type golden struct {
	PlatformNamespace       string `json:"platform_namespace"`
	WorkerControllerSubject string `json:"worker_controller_subject"`
	StreamPollUpdate        string `json:"stream_poll_update"`
	StreamOffsetQuery       string `json:"stream_offset_query"`
	Relay                   []struct {
		Namespace  string `json:"namespace"`
		JobID      string `json:"job_id"`
		WorkflowID string `json:"workflow_id"`
		Subject    string `json:"subject"`
	} `json:"relay"`
}

func readGolden(t *testing.T) golden {
	t.Helper()
	raw, err := os.ReadFile("testdata/principals.json")
	if err != nil {
		t.Fatal(err)
	}
	var g golden
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&g); err != nil {
		t.Fatal(err)
	}
	if len(g.Relay) == 0 {
		t.Fatal("the golden file has no relay vectors")
	}
	return g
}

func TestPrincipalsGolden(t *testing.T) {
	g := readGolden(t)
	for name, pair := range map[string][2]string{
		"platform_namespace":        {kontra.PlatformNamespace, g.PlatformNamespace},
		"worker_controller_subject": {kontra.ControllerSubject, g.WorkerControllerSubject},
		"stream_poll_update":        {kontra.StreamPollUpdate, g.StreamPollUpdate},
		"stream_offset_query":       {kontra.StreamOffsetQuery, g.StreamOffsetQuery},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: Go says %q, the golden file %q", name, pair[0], pair[1])
		}
	}
	for _, v := range g.Relay {
		if "job-"+v.JobID != v.WorkflowID {
			t.Errorf("%+v: the workflow id is not job-<job id>", v)
		}
		if got := kontra.RelaySubject(v.Namespace, v.WorkflowID); got != v.Subject {
			t.Errorf("RelaySubject(%q, %q) = %q, the golden file says %q", v.Namespace, v.WorkflowID, got, v.Subject)
		}
		// And the authorizer allows exactly that subject its job's poll.
		claims := mustJWT(v.Subject)
		if got := decide(t, kontra.New(), claims, updateWF, v.Namespace, upd(v.Namespace, v.WorkflowID, g.StreamPollUpdate)); got != allow {
			t.Errorf("%s: its own poll was %s", v.Subject, show(got))
		}
	}
}

// TestRelaySubjectHasOneSplit proves RelaySubject injective over what the gate signs: any
// request (ns', wf') with RelaySubject(ns', wf') == T is a split of T after "relay/" at a
// "/", and a T built from a handle and a job workflow id has exactly one "/" there.
func TestRelaySubjectHasOneSplit(t *testing.T) {
	for _, v := range readGolden(t).Relay {
		rest, ok := strings.CutPrefix(v.Subject, "relay/")
		if !ok {
			t.Fatalf("%q does not start with relay/", v.Subject)
		}
		splits := 0
		for i := range rest {
			if rest[i] != '/' {
				continue
			}
			splits++
			ns, wf := rest[:i], rest[i+1:]
			if kontra.RelaySubject(ns, wf) != v.Subject || ns != v.Namespace || wf != v.WorkflowID {
				t.Fatalf("%q splits as (%q, %q)", v.Subject, ns, wf)
			}
		}
		if splits != 1 {
			t.Fatalf("%q: %d splits", v.Subject, splits)
		}
	}
}

// handleRE is the Go twin of kontra.contract.names._HANDLE (with HANDLE_MAX = 32), used by
// this test only: nothing in the authorizer validates a handle.
var handleRE = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)

func isHandle(s string) bool { return len(s) <= 32 && handleRE.MatchString(s) }

// TestNonPartySubjectsAreNotHandles: a party token's subject is its handle, so no party
// token can ever carry the controller's or a relay's subject.
func TestNonPartySubjectsAreNotHandles(t *testing.T) {
	for _, h := range []string{"acme", "a", "x-1-y2", "kontra-system", kontra.PlatformNamespace} {
		if !isHandle(h) {
			t.Fatalf("the handle twin refuses %q", h)
		}
	}
	for _, h := range []string{"Acme", "acme-", "a--b", "1acme", "acme\n", strings.Repeat("a", 33)} {
		if isHandle(h) {
			t.Fatalf("the handle twin accepts %q", h)
		}
	}
	subjects := []string{kontra.ControllerSubject}
	for _, v := range readGolden(t).Relay {
		subjects = append(subjects, v.Subject)
	}
	for _, s := range subjects {
		if isHandle(s) || !strings.Contains(s, "/") {
			t.Errorf("%q could be a party's subject", s)
		}
	}
}

// TestRelayDoesNotMutateRequest: the request the authorizer reads is the one the handler
// then receives (interceptor.go hands the same object), so reading it must change nothing.
func TestRelayDoesNotMutateRequest(t *testing.T) {
	for _, req := range []proto.Message{
		upd("acme", jobJ, kontra.StreamPollUpdate),
		upd("acme", jobK, "approve"),
		qry("acme", jobJ, kontra.StreamOffsetQuery),
		qry("other", jobJ, "__stack_trace"),
		updateWithStart("acme", jobJ, kontra.StreamPollUpdate),
		&ws.UpdateWorkflowExecutionRequest{Namespace: "acme"}, // nothing set below the top
		&ws.QueryWorkflowRequest{Namespace: "acme"},
	} {
		before := proto.Clone(req)
		for _, claims := range []*authorization.Claims{relayJ, controller, acmeWorker, emptyJWT} {
			for _, apiName := range []string{updateWF, query, multiOp} {
				decide(t, kontra.New(), claims, apiName, "acme", req)
			}
		}
		if !proto.Equal(before, req) {
			t.Fatalf("the authorizer changed %v", req)
		}
	}
}

// TestTemporalHandsTheDecodedRequest runs Temporal's own authorization interceptor in front
// of Kontra's authorizer, as the frontend does, with a relay token through the real claim
// mapper: the request the relay rule reads is the decoded message itself, the one the
// handler then receives. A Temporal upgrade that handed the authorizer a copy, a wrapper or
// nothing would make the relay rule deny everything (it fails closed); this says so.
func TestTemporalHandsTheDecodedRequest(t *testing.T) {
	var seen any
	recorder := recordingAuthorizer{inner: kontra.New(), seen: &seen}
	mapper := authorization.NewDefaultJWTClaimMapper(keys{}, &config.Authorization{}, log.NewNoopLogger())
	off := func() bool { return false }
	interceptor := authorization.NewInterceptor(mapper, recorder, metrics.NoopMetricsHandler,
		log.NewNoopLogger(), noNamespaces{}, nil, "", "", off, off, nil, off)
	token := signJWT(jwt.MapClaims{"sub": kontra.RelaySubject("acme", jobJ)})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	info := &grpc.UnaryServerInfo{FullMethod: updateWF}

	var handled any
	handler := func(_ context.Context, req any) (any, error) { handled = req; return "ok", nil }
	req := upd("acme", jobJ, kontra.StreamPollUpdate)
	if _, err := interceptor.Intercept(ctx, req, info, handler); err != nil {
		t.Fatalf("the relay's own poll was refused: %v", err)
	}
	if seen != any(req) || handled != any(req) {
		t.Fatalf("the authorizer saw %p and the handler %p, not the request %p", seen, handled, req)
	}

	handled = nil
	_, err := interceptor.Intercept(ctx, upd("acme", jobK, kontra.StreamPollUpdate), info, handler)
	var denied *serviceerror.PermissionDenied
	if !errors.As(err, &denied) || handled != nil {
		t.Fatalf("another job's poll: got %v, handler called: %v", err, handled != nil)
	}
}

type recordingAuthorizer struct {
	inner authorization.Authorizer
	seen  *any
}

func (r recordingAuthorizer) Authorize(ctx context.Context, c *authorization.Claims, t *authorization.CallTarget) (authorization.Result, error) {
	*r.seen = t.Request
	return r.inner.Authorize(ctx, c, t)
}

type noNamespaces struct{}

func (noNamespaces) Exists(namespace.Name) error { return errors.New("not checked here") }

// --- helpers ------------------------------------------------------------------------

func decide(t *testing.T, a authorization.Authorizer, claims *authorization.Claims, apiName, ns string, req any) authorization.Decision {
	t.Helper()
	res, err := a.Authorize(context.Background(), claims, &authorization.CallTarget{APIName: apiName, Namespace: ns, Request: req})
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

// streamPublish is the workflow stream's publish signal
// (temporalio.contrib.workflow_streams _PUBLISH_SIGNAL), which a relay may never send.
const streamPublish = "__temporal_workflow_stream_publish"

func upd(ns, wf, name string) *ws.UpdateWorkflowExecutionRequest {
	return &ws.UpdateWorkflowExecutionRequest{
		Namespace:         ns,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf},
		Request:           &updatepb.Request{Meta: &updatepb.Meta{UpdateId: "u"}, Input: &updatepb.Input{Name: name}},
	}
}

func qry(ns, wf, queryType string) *ws.QueryWorkflowRequest {
	return &ws.QueryWorkflowRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: wf},
		Query:     &querypb.WorkflowQuery{QueryType: queryType},
	}
}

func sig(ns, wf, name string) *ws.SignalWorkflowExecutionRequest {
	return &ws.SignalWorkflowExecutionRequest{
		Namespace: ns, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf}, SignalName: name,
	}
}

func updateWithStart(ns, wf, name string) *ws.ExecuteMultiOperationRequest {
	return &ws.ExecuteMultiOperationRequest{Namespace: ns, Operations: []*ws.ExecuteMultiOperationRequest_Operation{
		{Operation: &ws.ExecuteMultiOperationRequest_Operation_StartWorkflow{StartWorkflow: &ws.StartWorkflowExecutionRequest{
			Namespace: ns, WorkflowId: wf,
		}}},
		{Operation: &ws.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{UpdateWorkflow: upd(ns, wf, name)}},
	}}
}

// inputs maps every unary method of the services the frontend registers to its request type.
var inputs = func() map[string]protoreflect.MessageDescriptor {
	m := map[string]protoreflect.MessageDescriptor{}
	for _, file := range []protoreflect.FileDescriptor{
		ws.File_temporal_api_workflowservice_v1_service_proto,
		ops.File_temporal_api_operatorservice_v1_service_proto,
		adm.File_temporal_server_api_adminservice_v1_service_proto,
		healthpb.File_grpc_health_v1_health_proto,
	} {
		for i := 0; i < file.Services().Len(); i++ {
			svc := file.Services().Get(i)
			for j := 0; j < svc.Methods().Len(); j++ {
				method := svc.Methods().Get(j)
				if !method.IsStreamingClient() && !method.IsStreamingServer() {
					m["/"+string(svc.FullName())+"/"+string(method.Name())] = method.Input()
				}
			}
		}
	}
	return m
}()

// requestFor is the request Temporal would hand the authorizer for apiName, naming ns and
// the workflow wf, with the stream's handler names for an Update or a Query: the most
// favourable request a relay could send.
func requestFor(apiName, ns, wf string) any {
	return requestWith(apiName, ns, wf, kontra.StreamPollUpdate, kontra.StreamOffsetQuery)
}

// requestWith builds apiName's typed request (nil for a stream, which has none) with its
// namespace, every workflow id it names, and the given update name or query type. The
// Nexus HTTP routes hand the authorizer a matching-service dispatch request.
func requestWith(apiName, ns, wf, update, queryType string) any {
	if strings.HasPrefix(apiName, nexusPrefix) {
		if apiName == completeNexus || apiName == completeChasm {
			return nil
		}
		return &matchingservice.DispatchNexusTaskRequest{}
	}
	input, ok := inputs[apiName]
	if !ok {
		return nil
	}
	mt, err := protoregistry.GlobalTypes.FindMessageByName(input.FullName())
	if err != nil {
		panic(err)
	}
	msg := mt.New()
	set := func(m protoreflect.Message, field, value string) {
		if f := m.Descriptor().Fields().ByName(protoreflect.Name(field)); f != nil && f.Kind() == protoreflect.StringKind && !f.IsList() {
			m.Set(f, protoreflect.ValueOfString(value))
		}
	}
	set(msg, "namespace", ns)
	set(msg, "workflow_id", wf)
	for _, name := range []string{"workflow_execution", "execution"} {
		if f := msg.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil && f.Kind() == protoreflect.MessageKind && !f.IsList() {
			set(msg.Mutable(f).Message(), "workflow_id", wf)
		}
	}
	switch req := msg.Interface().(type) {
	case *ws.UpdateWorkflowExecutionRequest:
		req.Request = &updatepb.Request{Meta: &updatepb.Meta{UpdateId: "u"}, Input: &updatepb.Input{Name: update}}
	case *ws.QueryWorkflowRequest:
		req.Query = &querypb.WorkflowQuery{QueryType: queryType}
	case *ws.PollWorkflowExecutionUpdateRequest:
		req.UpdateRef = &updatepb.UpdateRef{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf}, UpdateId: "u"}
	}
	return msg.Interface()
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
// claim mapper, exactly as the public frontend does. With no permissions the token has no
// "permissions" claim at all, as the gate mints a relay's or the controller's.
func mustJWT(subject string, permissions ...string) *authorization.Claims {
	claims := jwt.MapClaims{"sub": subject}
	if len(permissions) > 0 {
		claims["permissions"] = permissions
	}
	return mapJWT(signJWT(claims))
}

// mustJWTEmptyPermissions is mustJWT with the claim present and empty: "permissions": [].
func mustJWTEmptyPermissions(subject string) *authorization.Claims {
	return mapJWT(signJWT(jwt.MapClaims{"sub": subject, "permissions": []string{}}))
}

func signJWT(claims jwt.MapClaims) string {
	claims["exp"] = time.Now().Add(time.Hour).Unix()
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = "test"
	signed, err := token.SignedString(signingKey)
	if err != nil {
		panic(err)
	}
	return signed
}

func mapJWT(signed string) *authorization.Claims {
	mapper := authorization.NewDefaultJWTClaimMapper(keys{}, &config.Authorization{}, log.NewNoopLogger())
	claims, err := mapper.GetClaims(&authorization.AuthInfo{AuthToken: "Bearer " + signed})
	if err != nil {
		panic(err)
	}
	return claims
}
