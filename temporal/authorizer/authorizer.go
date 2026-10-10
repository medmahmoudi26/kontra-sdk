// Package authorizer is the policy Kontra's Temporal frontend enforces (PRD §10.4,
// decisions I3). It is public so that every seller and buyer can read exactly what a
// token lets them do. Anything this file does not allow is denied.
//
// Callers reach Temporal through one of two listeners.
//
// The internal frontend is never exposed outside the host or container network. Temporal
// maps every caller there to its own internal principal: the gate (namespaces, Nexus
// endpoints, watching jobs) and Temporal itself (the system worker, history dispatching
// Nexus tasks and delivering Nexus completions). That principal may do anything.
//
// On the public frontend, sellers and buyers present a short-lived JWT, verified by
// Temporal's default claim mapper against Kontra's JWKS. The one permission that means
// anything is "<namespace>:worker": the WorkflowService calls in AllowedAPIs, in that
// namespace only. A buyer holds the same kind of token for its own namespace, because any
// party can sell and buy. Every other role ("admin", "write", "read", and any
// "temporal-system:..." role) grants nothing here.
//
// Decisions compare whole strings for equality or set membership. Nothing here splits an
// API name, a namespace or an endpoint name, and nothing reads a request body.
package authorizer

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	ws "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/primitives"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// PublicAPIs may be called by anyone, with or without a token. They reveal nothing about
// any namespace, and SDK clients call GetSystemInfo when they connect.
var PublicAPIs = setOf(
	healthpb.Health_Check_FullMethodName,
	ws.WorkflowService_GetSystemInfo_FullMethodName,
	ws.WorkflowService_GetClusterInfo_FullMethodName,
)

// AllowedAPIs are the only calls a worker token may make, and only in its own namespace:
// what the Python SDK's workers and clients use, standalone Nexus operations included
// (that is how a script hires, decisions I4).
//
// Absent on purpose: every OperatorService and AdminService call (namespaces, search
// attributes, clusters, Nexus endpoints), the frontend's Nexus HTTP API, namespace
// administration, batch operations, schedules, resets, deletes, worker deployments and
// versioning rules, and every call that does not name a namespace.
var AllowedAPIs = setOf(
	ws.WorkflowService_DescribeNamespace_FullMethodName,

	// Workers: poll, respond, heartbeat.
	ws.WorkflowService_PollWorkflowTaskQueue_FullMethodName,
	ws.WorkflowService_RespondWorkflowTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondWorkflowTaskFailed_FullMethodName,
	ws.WorkflowService_RespondQueryTaskCompleted_FullMethodName,
	ws.WorkflowService_PollActivityTaskQueue_FullMethodName,
	ws.WorkflowService_RecordActivityTaskHeartbeat_FullMethodName,
	ws.WorkflowService_RecordActivityTaskHeartbeatById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCompletedById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskFailed_FullMethodName,
	ws.WorkflowService_RespondActivityTaskFailedById_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCanceled_FullMethodName,
	ws.WorkflowService_RespondActivityTaskCanceledById_FullMethodName,
	ws.WorkflowService_PollNexusTaskQueue_FullMethodName,
	ws.WorkflowService_RespondNexusTaskCompleted_FullMethodName,
	ws.WorkflowService_RespondNexusTaskFailed_FullMethodName,
	ws.WorkflowService_RecordWorkerHeartbeat_FullMethodName,
	ws.WorkflowService_ShutdownWorker_FullMethodName,

	// Clients: workflows.
	ws.WorkflowService_StartWorkflowExecution_FullMethodName,
	ws.WorkflowService_SignalWorkflowExecution_FullMethodName,
	ws.WorkflowService_SignalWithStartWorkflowExecution_FullMethodName,
	ws.WorkflowService_ExecuteMultiOperation_FullMethodName,
	ws.WorkflowService_UpdateWorkflowExecution_FullMethodName,
	ws.WorkflowService_PollWorkflowExecutionUpdate_FullMethodName,
	ws.WorkflowService_QueryWorkflow_FullMethodName,
	ws.WorkflowService_DescribeWorkflowExecution_FullMethodName,
	ws.WorkflowService_GetWorkflowExecutionHistory_FullMethodName,
	ws.WorkflowService_RequestCancelWorkflowExecution_FullMethodName,
	ws.WorkflowService_TerminateWorkflowExecution_FullMethodName,
	ws.WorkflowService_ListWorkflowExecutions_FullMethodName,
	ws.WorkflowService_CountWorkflowExecutions_FullMethodName,

	// Clients: standalone Nexus operations.
	ws.WorkflowService_StartNexusOperationExecution_FullMethodName,
	ws.WorkflowService_DescribeNexusOperationExecution_FullMethodName,
	ws.WorkflowService_PollNexusOperationExecution_FullMethodName,
	ws.WorkflowService_RequestCancelNexusOperationExecution_FullMethodName,
	ws.WorkflowService_TerminateNexusOperationExecution_FullMethodName,
	ws.WorkflowService_ListNexusOperationExecutions_FullMethodName,
	ws.WorkflowService_CountNexusOperationExecutions_FullMethodName,
)

// Authorizer implements Temporal's authorization.Authorizer with the policy above.
type Authorizer struct{}

var _ authorization.Authorizer = Authorizer{}

// New returns Kontra's authorizer.
func New() Authorizer { return Authorizer{} }

// Authorize decides one call. Temporal calls it before every frontend request: gRPC
// (unary and streaming, which carry no namespace), the HTTP API, and the frontend's Nexus
// HTTP API alike. The namespace it is given is the one Temporal acts on: a request that
// names none but carries a task token is given the token's before this runs, a token must
// belong to the namespace the request names (frontend.enableTokenNamespaceEnforcement),
// the operations of a multi-operation must share it, and workflow commands that target
// another namespace are refused by history (system.enableCrossNamespaceCommands is off).
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
	case !AllowedAPIs[target.APIName]:
		return deny("this API is not available to tokens: " + target.APIName), nil
	case target.Namespace == "" || target.Namespace == primitives.SystemLocalNamespace:
		return deny("this call must name the token's own namespace"), nil
	case claims.Namespaces[target.Namespace]&authorization.RoleWorker == 0:
		return deny("the token has no worker role in namespace " + target.Namespace), nil
	}
	return allow(claims), nil
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
