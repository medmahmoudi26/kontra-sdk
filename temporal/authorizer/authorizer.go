// Package authorizer is the policy Kontra's Temporal frontend enforces (PRD §10.4,
// decisions I3, I30). It is public so that every seller and buyer can read exactly what a
// token lets them do. Anything this file does not allow is denied.
//
// Callers reach Temporal through one of two listeners.
//
// The internal frontend is never exposed outside the host or container network. Temporal
// maps every caller there to its own internal principal: the gate (namespaces, Nexus
// endpoints, watching jobs) and Temporal itself (the system worker, history dispatching
// Nexus tasks and delivering Nexus completions). That principal may do anything.
//
// On the public frontend every caller presents a short-lived JWT, verified by Temporal's
// default claim mapper against Kontra's JWKS, that names a session the gate can revoke.
// Three kinds of token mean anything, told apart by the claims the mapper produces, in
// this order:
//
//   - A party token carries a permission: a permission string the mapper turns into a
//     namespace key (any "<namespace>:<role>", even a role it does not know) or into a
//     system role it knows ("temporal-system:" with "read", "write", "admin" or
//     "worker"). The one that grants anything is "<namespace>:worker": the WorkflowService
//     calls in AllowedAPIs, in that namespace only. A seller's worker token, a seller's
//     API-key session, a buyer (a worker of its own namespace, because any party can sell
//     and buy) and the dev party all hold this token and are identical on the wire. Every
//     other role ("admin", "write", "read", an unknown role in a namespace, a
//     "temporal-system:" role the mapper knows) grants nothing. A token with a permission
//     is judged by this rule alone, whatever its subject says.
//   - The worker controller (KON-15) carries no permission and the subject
//     ControllerSubject: the worker-deployment calls in ControllerAPIs, in
//     PlatformNamespace only.
//   - The gate's stream relay carries no permission and the subject RelaySubject(namespace,
//     job workflow id): the stream's poll Update and offset Query on that one workflow.
//
// A permission string the mapper drops is no permission: "temporal-system:" with a role it
// does not know (it maps to no role and creates no namespace key), an entry with no ":", an
// entry that is not a string, a "permissions" claim that is not a list. A token carrying
// only those is judged by its subject like one that carries none, so the controller's or
// a relay's subject with such an entry gets that principal's calls.
//
// Party tokens and the other two are told apart twice: the gate mints a party token with
// a permission and the other two with none, and a party's subject is a handle, which can
// never contain the "/" both other subjects contain. The second holds whatever the mapper
// does with a token's permissions; the first holds as long as the gate writes only
// permissions the mapper keeps, which is why decisions I30 has the gate mint each
// principal's token with its own function and no role parameter.
//
// Decisions compare whole strings for equality or set membership; nothing here splits an
// API name, a namespace, an endpoint name or a subject. One rule reads the request: a
// stream relay's, which takes two typed fields of the message Temporal already
// deserialized (CallTarget.Request), the workflow id and the update name or query type. It
// compares each, whole, with a value it constructs. A request of any other type is denied.
package authorizer

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/primitives"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	// PlatformNamespace is Kontra's own Temporal namespace: the gate's platform workflows
	// and the workers the worker controller rolls out (KON-15). It is a reserved handle
	// (kontra.contract.names.RESERVED_HANDLES, PLATFORM_NAMESPACE), so no party can own it,
	// and it is a constant here so that every seller can read where the controller acts.
	PlatformNamespace = "kontra-system"

	// ControllerSubject is the subject of the temporal-worker-controller's token
	// (kontra.contract.names.WORKER_CONTROLLER_SUBJECT). It contains "/", which no handle
	// can, so no party token's subject is ever equal to it.
	ControllerSubject = "platform/worker-controller"

	// StreamPollUpdate and StreamOffsetQuery are the two workflow-stream handlers a relay
	// may use (temporalio.contrib.workflow_streams _POLL_UPDATE and _OFFSET_QUERY, pinned
	// by tests/contract/test_names.py). The stream's publish signal is deliberately not
	// one of them: a relay reads a stream and never publishes to it. Each poll's outcome is
	// still written to the job's history, which this policy cannot bound (decisions I30).
	StreamPollUpdate  = "__temporal_workflow_stream_poll"
	StreamOffsetQuery = "__temporal_workflow_stream_offset"
)

// RelaySubject is the subject of the gate's stream relay for the workflow workflowID in
// namespace: the twin of kontra.contract.names.relay_subject (decisions I1). The
// authorizer builds it from a request's namespace and workflow id and compares it, whole,
// with the token's subject. Nothing splits a subject.
//
// It is injective over what the gate signs: the namespace is a handle and the workflow id
// is job-<32 hex>, neither contains "/", so the text after "relay/" holds exactly one "/",
// and a request that rebuilds the same subject names the same namespace and workflow.
func RelaySubject(namespace, workflowID string) string {
	return "relay/" + namespace + "/" + workflowID
}

// PublicAPIs may be called by anyone, with or without a token. They reveal nothing about
// any namespace, and SDK clients call GetSystemInfo when they connect.
var PublicAPIs = setOf(
	// Load balancers, and the SDKs' health check; reveals serving status only.
	healthpb.Health_Check_FullMethodName,
	// Every SDK calls it on connect for the server's capabilities; cluster-wide, no
	// namespace data.
	ws.WorkflowService_GetSystemInfo_FullMethodName,
	// The SDK and CLI version handshake: the cluster's name and version only.
	ws.WorkflowService_GetClusterInfo_FullMethodName,
)

// AllowedAPIs are the only calls a party token (a "<namespace>:worker" token) may make,
// and only in its own namespace: what the Python SDK's workers and clients use, standalone
// Nexus operations included (that is how a script hires, decisions I4). A request that
// names another namespace, none, "temporal-system" or PlatformNamespace is refused.
// Cross-namespace workflow commands are refused by history
// (system.enableCrossNamespaceCommands is pinned off), task tokens are bound to their
// namespace (frontend.enableTokenNamespaceEnforcement is pinned on), and every completion
// callback a request carries is restricted by the callbackguard package.
//
// A party steers its own workflows (decisions I30): updates, queries and signals,
// including those between its own workflows, and terminate and cancel. Escrow reads
// nothing these calls can write: a job's facts are the server-recorded status of its
// pinned chain, provenance from the buyer's records, the clock and the buyer's gate calls.
//
// Absent on purpose: every OperatorService and AdminService call (namespaces, search
// attributes, clusters, Nexus endpoints), the frontend's Nexus HTTP API, namespace
// administration, batch operations, schedules, resets, deletes, worker deployments and
// versioning rules, and every call that does not name a namespace. Several of these
// denials protect escrow and must not be relaxed: a reset can bring a terminated pinned
// run back to life with its completion callbacks; a deleted workflow, Nexus operation or
// shortened retention turns a buyer's record of a delivery into an unverified refund;
// batch operations and schedules act as Temporal's internal principal, skipping this
// policy and the callback guard; compute config makes the server itself provision and
// invoke workers on a cloud provider.
var AllowedAPIs = setOf(
	// SDK workers read their own namespace's capabilities at start; own metadata only.
	ws.WorkflowService_DescribeNamespace_FullMethodName,

	// Workers: the party's worker runs its own workflows...
	ws.WorkflowService_PollWorkflowTaskQueue_FullMethodName,
	ws.WorkflowService_RespondWorkflowTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondWorkflowTaskFailed_FullMethodName,
	// ...answers queries on them...
	ws.WorkflowService_RespondQueryTaskCompleted_FullMethodName,
	// ...runs its own activities, by task token (namespace-bound)...
	ws.WorkflowService_PollActivityTaskQueue_FullMethodName,
	ws.WorkflowService_RecordActivityTaskHeartbeat_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondActivityTaskFailed_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCanceled_FullMethodName,
	// ...or completes them asynchronously by workflow id, in its own namespace
	// (temporalio's AsyncActivityHandle)...
	ws.WorkflowService_RecordActivityTaskHeartbeatById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCompletedById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskFailedById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCanceledById_FullMethodName,
	// ...serves its Nexus handlers (quote and execute; callbackguard checks the start it
	// makes to complete one)...
	ws.WorkflowService_PollNexusTaskQueue_FullMethodName,
	ws.WorkflowService_RespondNexusTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondNexusTaskFailed_FullMethodName,
	// ...and reports its own lifecycle (temporalio 1.34 sends both); own queues only.
	ws.WorkflowService_RecordWorkerHeartbeat_FullMethodName,
	ws.WorkflowService_ShutdownWorker_FullMethodName,

	// Clients: workflows of the party's own namespace.
	// The handler starts job-<id> (decisions I6); a buyer starts its own workflows.
	ws.WorkflowService_StartWorkflowExecution_FullMethodName,
	// Signals to its own workflows, between its own workflows too: a WorkflowStream's
	// publish signal from its activities, a harness (seller self-access).
	ws.WorkflowService_SignalWorkflowExecution_FullMethodName,
	// Start plus signal, on its own workflows.
	ws.WorkflowService_SignalWithStartWorkflowExecution_FullMethodName,
	// Update-with-start; every operation must name the request's namespace.
	ws.WorkflowService_ExecuteMultiOperation_FullMethodName,
	// Updates to its own workflows: its own stream subscriptions, a harness's approvals
	// from the dev console's session pane (seller self-access; callbacks inside are
	// checked by callbackguard).
	ws.WorkflowService_UpdateWorkflowExecution_FullMethodName,
	// The SDK polls an update handle's outcome; a stock WorkflowStreamClient needs it.
	ws.WorkflowService_PollWorkflowExecutionUpdate_FullMethodName,
	// Queries on its own workflows, the stream offset query included.
	ws.WorkflowService_QueryWorkflow_FullMethodName,
	// handle.describe(), the dev graph page (I28), following a continue-as-new.
	ws.WorkflowService_DescribeWorkflowExecution_FullMethodName,
	// handle.result() long-polls history; the graph page and the session pane read it.
	ws.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
	// Cancel its own workflows. Escrow reads who canceled a job from the buyer's records
	// (I16), so a seller's cancel counts against the seller.
	ws.WorkflowService_RequestCancelWorkflowExecution_FullMethodName,
	// Stop its own workflows. On a job that is "seller workflow terminated", a refund
	// against the seller: it harms only itself.
	ws.WorkflowService_TerminateWorkflowExecution_FullMethodName,
	// Visibility of its own namespace (`temporal workflow list`, `kontra status`).
	ws.WorkflowService_ListWorkflowExecutions_FullMethodName,
	ws.WorkflowService_CountWorkflowExecutions_FullMethodName,

	// A seller's own backlog. DescribeTaskQueue with report_stats returns
	// ApproximateBacklogCount/Age and the add and dispatch rates per task-queue type:
	// metadata, no payloads, and only for a queue in the token's own namespace. Without
	// it a seller cannot run `temporal task-queue describe`, and KEDA's Temporal scaler
	// -- which calls exactly this RPC -- cannot scale workers against Kontra's frontend.
	ws.WorkflowService_DescribeTaskQueue_FullMethodName,

	// Clients: standalone Nexus operations in its own namespace, how a script hires (I4).
	// Outward Nexus is allowed by design, since any party can buy: the target handler
	// refuses an execute without a gate-signed hold (I10). Terminating its own operation
	// does not cancel the seller's run, and the operation's record survives (I16).
	ws.WorkflowService_StartNexusOperationExecution_FullMethodName,
	ws.WorkflowService_DescribeNexusOperationExecution_FullMethodName,
	ws.WorkflowService_PollNexusOperationExecution_FullMethodName,
	ws.WorkflowService_RequestCancelNexusOperationExecution_FullMethodName,
	ws.WorkflowService_TerminateNexusOperationExecution_FullMethodName,
	ws.WorkflowService_ListNexusOperationExecutions_FullMethodName,
	ws.WorkflowService_CountNexusOperationExecutions_FullMethodName,
)

// ControllerAPIs are the only calls the worker controller may make besides the public
// ones (GetSystemInfo to dial, Health/Check), and only in PlatformNamespace: exactly what
// temporal-worker-controller v1.13.0 calls to roll out Kontra's own workers. Each entry
// names the controller code that needs it.
//
// Absent on purpose: StartWorkflowExecution and DescribeWorkflowExecution, which the
// controller uses only for rollout gate workflows (a stolen controller credential could
// otherwise start any platform workflow with any input), and the compute-config calls
// (CreateWorkerDeploymentVersion, UpdateWorkerDeploymentVersionComputeConfig,
// ValidateWorkerDeploymentVersionComputeConfig), which make the server itself provision
// and invoke workers on a cloud provider with its own identity.
var ControllerAPIs = setOf(
	// Reads the platform deployment's routing state (internal/temporal/worker_deployment.go:96,
	// internal/controller/worker_controller.go:640).
	ws.WorkflowService_DescribeWorkerDeployment_FullMethodName,
	// A version's status, drainage and task queues (worker_deployment.go:158,181,300,371).
	ws.WorkflowService_DescribeWorkerDeploymentVersion_FullMethodName,
	// Promotes a version, the rollout itself (worker_controller.go:679,
	// internal/controller/execplan.go:359).
	ws.WorkflowService_SetWorkerDeploymentCurrentVersion_FullMethodName,
	// Progressive rollout (worker_controller.go:657, execplan.go:381).
	ws.WorkflowService_SetWorkerDeploymentRampingVersion_FullMethodName,
	// Claims management of the deployment, so manual edits are flagged (execplan.go:328).
	ws.WorkflowService_SetWorkerDeploymentManager_FullMethodName,
	// Records its rollout bookkeeping on the version (execplan.go:402).
	ws.WorkflowService_UpdateWorkerDeploymentVersionMetadata_FullMethodName,
	// Retires drained versions (worker_controller.go:718, execplan.go:854).
	ws.WorkflowService_DeleteWorkerDeploymentVersion_FullMethodName,
	// Removes the deployment when its Kubernetes resource is deleted (worker_controller.go:729).
	ws.WorkflowService_DeleteWorkerDeployment_FullMethodName,
	// Checks the workflow, activity and Nexus queues for pollers before promoting or
	// retiring a version (worker_deployment.go:476-480).
	ws.WorkflowService_DescribeTaskQueue_FullMethodName,
	// Waits for a version's pinned, running workflows to drain before deleting it
	// (execplan.go:907, a query on TemporalWorkerDeploymentVersion).
	ws.WorkflowService_CountWorkflowExecutions_FullMethodName,
)

// RelayAPIs are the only two calls a stream relay can make. This set documents the rule
// and feeds the tests: the decision itself is made by relay's type switch, which also
// requires the stream handler's name and the relay's own workflow.
var RelayAPIs = setOf(
	// The stream's subscribe long-poll, with update name StreamPollUpdate.
	ws.WorkflowService_UpdateWorkflowExecution_FullMethodName,
	// The stream's length, to start at the tail or resume, with query type
	// StreamOffsetQuery.
	ws.WorkflowService_QueryWorkflow_FullMethodName,
)

// Authorizer implements Temporal's authorization.Authorizer with the policy above.
type Authorizer struct{}

var _ authorization.Authorizer = Authorizer{}

// New returns Kontra's authorizer.
func New() Authorizer { return Authorizer{} }

// Authorize decides one call. Temporal calls it before every frontend request: gRPC
// (unary and streaming, which carry no namespace and no request), the HTTP API, and the
// frontend's Nexus HTTP API alike. The namespace it is given is the one Temporal acts on:
// a request that names none but carries a task token is given the token's before this
// runs, a token must belong to the namespace the request names
// (frontend.enableTokenNamespaceEnforcement), the operations of a multi-operation must
// share it, and workflow commands that target another namespace are refused by history
// (system.enableCrossNamespaceCommands is off). For a unary call the request it is given
// is the decoded message the handler then receives, not a copy.
//
// One thing this cannot see: a Nexus completion travels from the handler's namespace to
// the caller's as Temporal's internal principal, named only by an unsigned token. The
// callbackguard package checks those tokens where a handler hands them to Temporal.
func (Authorizer) Authorize(
	_ context.Context, claims *authorization.Claims, target *authorization.CallTarget,
) (authorization.Result, error) {
	switch {
	case IsInternal(claims):
		return allow(claims), nil
	case PublicAPIs[target.APIName]:
		return allow(claims), nil
	case claims == nil:
		return deny("a token is required"), nil
	case hasPermissions(claims):
		return party(claims, target), nil
	case claims.Subject == ControllerSubject:
		return controller(claims, target), nil
	}
	return relay(claims, target), nil
}

// hasPermissions reports whether the token carried any permission. Temporal's default
// claim mapper creates a namespace's key even for a role it does not know
// (default_jwt_claim_mapper.go: an unknown role is RoleUndefined, but the key is set), so
// the test is "any namespace key or any system role", not "any role bit": a token that
// says "acme:relay" is a party token with no role, never a relay. The system clause keeps
// "temporal-system:admin" next to the controller's or a relay's subject in the party rule.
// What the mapper drops, "temporal-system:" with a role it does not know included, sets
// neither, and is no permission.
func hasPermissions(claims *authorization.Claims) bool {
	return claims.System != authorization.RoleUndefined || len(claims.Namespaces) > 0
}

// partyNamespace reports whether ns can be a party's namespace: not empty, not
// Temporal's own and not Kontra's.
func partyNamespace(ns string) bool {
	return ns != "" && ns != primitives.SystemLocalNamespace && ns != PlatformNamespace
}

// party is the rule for every token that carries a permission: AllowedAPIs, in a
// namespace where the token holds the worker role. PlatformNamespace is not excluded
// here: a "kontra-system:worker" token (only the platform mints one) is a platform worker.
func party(claims *authorization.Claims, t *authorization.CallTarget) authorization.Result {
	switch {
	case !AllowedAPIs[t.APIName]:
		return deny("this API is not available to tokens: " + t.APIName)
	case t.Namespace == "" || t.Namespace == primitives.SystemLocalNamespace:
		return deny("this call must name the token's own namespace")
	case claims.Namespaces[t.Namespace]&authorization.RoleWorker == 0:
		return deny("the token has no worker role in namespace " + t.Namespace)
	}
	return allow(claims)
}

// controller is the worker controller's rule: ControllerAPIs, in PlatformNamespace only.
func controller(claims *authorization.Claims, t *authorization.CallTarget) authorization.Result {
	switch {
	case t.Namespace != PlatformNamespace:
		return deny("the worker controller acts in the platform namespace only")
	case !ControllerAPIs[t.APIName]:
		return deny("this API is not available to the worker controller: " + t.APIName)
	}
	return allow(claims)
}

// relay is the stream relay's rule, reached by every token with no permission that is not
// the controller's. It reads the request's workflow id and handler name from the typed
// message, only for the two request types below, each under its own API name; any other
// request (another type, none for a stream, a Nexus HTTP dispatch, an update inside
// ExecuteMultiOperation) is denied. The subject is then compared, whole, with the one
// RelaySubject builds from the request: a token whose subject is anything else, empty
// included, is denied here too.
func relay(claims *authorization.Claims, t *authorization.CallTarget) authorization.Result {
	if !partyNamespace(t.Namespace) {
		return deny("a stream relay acts in a party's namespace only")
	}
	var workflowID, handler, want string
	switch req := t.Request.(type) {
	case *ws.UpdateWorkflowExecutionRequest:
		if t.APIName != ws.WorkflowService_UpdateWorkflowExecution_FullMethodName {
			return deny("this API is not available to a stream relay: " + t.APIName)
		}
		workflowID = req.GetWorkflowExecution().GetWorkflowId()
		handler, want = req.GetRequest().GetInput().GetName(), StreamPollUpdate
	case *ws.QueryWorkflowRequest:
		if t.APIName != ws.WorkflowService_QueryWorkflow_FullMethodName {
			return deny("this API is not available to a stream relay: " + t.APIName)
		}
		workflowID = req.GetExecution().GetWorkflowId()
		handler, want = req.GetQuery().GetQueryType(), StreamOffsetQuery
	default:
		return deny("this API is not available to a stream relay: " + t.APIName)
	}
	if handler != want {
		return deny("a stream relay may only poll the stream or read its offset")
	}
	if claims.Subject != RelaySubject(t.Namespace, workflowID) {
		return deny("the token is not this workflow's stream relay")
	}
	return allow(claims)
}

// IsInternal reports whether the claims are Temporal's internal principal, which only the
// internal frontend's claim mapper produces (authorization.NewInternalClaimMapper). A JWT
// never yields it: the JWT claim mapper always sets AuthType "jwt", whatever the token's
// subject says.
func IsInternal(claims *authorization.Claims) bool {
	return claims != nil &&
		claims.AuthType == authorization.InternalPrincipalType &&
		claims.Subject == authorization.InternalPrincipalName
}

func allow(claims *authorization.Claims) authorization.Result {
	result := authorization.Result{Decision: authorization.DecisionAllow}
	if claims != nil {
		result.Principal = &commonpb.Principal{Type: claims.AuthType, Name: claims.Subject}
	}
	return result
}

func deny(reason string) authorization.Result {
	return authorization.Result{Decision: authorization.DecisionDeny, Reason: "kontra: " + reason}
}

func setOf(names ...string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}
	return set
}
