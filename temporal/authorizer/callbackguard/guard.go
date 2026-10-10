// Package callbackguard closes a cross-tenant hole the Authorizer cannot see: one seller
// completing another seller's Nexus operation with a result of its choosing.
//
// Every Nexus handler receives the callback token of the operation it serves, and
// completes the operation by starting a workflow with that token in a "temporal://system"
// completion callback. The token is unsigned and names the caller's namespace, workflow,
// run and operation. Temporal v1.32.1 checks a completion against the operation's request
// id only when the token carries one (components/nexusoperations/completion.go,
// chasm/lib/nexusoperation/operation.go: `requestID != "" && ...`). So a seller hired by
// the same buyer workflow as acme can rewrite its own token to name acme's operation, drop
// the request id, and deliver any result as acme's; the completion then travels through
// the internal frontend as Temporal's own principal, which the Authorizer must allow.
//
// Every token Temporal issues carries a request id that only that operation's handler and
// caller know. So for any caller but Temporal's internal principal, this guard accepts a
// completion callback only in the one shape a Nexus handler ever needs: a Nexus callback to
// "temporal://system" whose single token carries a request id. A token can then complete
// only the operation it was issued for. It refuses every other URL too: Temporal does not
// validate callback URLs on every API that accepts them (UpdateWorkflowExecution, v1.32.1),
// and a callback is an HTTP request made by Temporal itself.
//
// It is deliberately separate from the Authorizer: the policy there never reads a request
// body; this is input validation of a token Temporal trusts.
package callbackguard

import (
	"context"
	"strings"
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/authorization"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	kontra "github.com/medmahmoudi26/kontra-sdk/temporal/authorizer"
)

// Intercept is a gRPC unary server interceptor for both frontends
// (temporal.WithChainedFrontendGrpcInterceptors). Temporal runs it after its
// authorization interceptor, so the caller's claims are in ctx and only authorized calls
// reach it. Temporal's internal principal (the control plane and Temporal itself, whose
// scheduler attaches its own "temporal://internal" callbacks) is not checked.
func Intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	claims, _ := ctx.Value(authorization.MappedClaims).(*authorization.Claims)
	if msg, ok := req.(proto.Message); ok && !kontra.IsInternal(claims) {
		if err := Check(msg); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

// Check returns PermissionDenied if msg carries, anywhere inside it, a completion callback
// other than a Nexus callback to "temporal://system" with exactly one token, and that
// token naming its operation's request id.
func Check(msg proto.Message) error {
	return visit(msg.ProtoReflect(), func(cb *commonpb.Callback) error {
		nexus := cb.GetNexus()
		if nexus == nil || nexus.GetUrl() != commonnexus.SystemCallbackURL {
			return deny("a completion callback must be a Nexus callback to " + commonnexus.SystemCallbackURL)
		}
		tokens := 0
		for key, value := range nexus.GetHeader() {
			// Temporal lowercases header keys before it stores them, keeping one of two
			// spellings at random, so every spelling counts.
			if strings.EqualFold(key, commonnexus.CallbackTokenHeader) {
				tokens++
				if !hasRequestID(value) {
					return deny("a completion callback token must name its operation's request id")
				}
			}
		}
		if tokens != 1 {
			return deny("a completion callback must carry exactly one token")
		}
		return nil
	})
}

func deny(reason string) error {
	return serviceerror.NewPermissionDenied("kontra: "+reason, "")
}

// hasRequestID decodes a token exactly as Temporal's completion handler does.
func hasRequestID(encoded string) bool {
	token, err := commonnexus.DecodeCallbackToken(encoded)
	if err != nil {
		return false
	}
	completion, err := commonnexus.NewCallbackTokenGenerator().DecodeCompletion(token)
	return err == nil && completion.GetRequestId() != ""
}

var callbackName = (&commonpb.Callback{}).ProtoReflect().Descriptor().FullName()

// visit calls fn on every Callback inside m, skipping message types that cannot contain one.
func visit(m protoreflect.Message, fn func(*commonpb.Callback) error) error {
	if m.Descriptor().FullName() == callbackName {
		cb, ok := m.Interface().(*commonpb.Callback)
		if !ok { // never the case for requests the frontends decode; refuse rather than skip
			return deny("unreadable completion callback")
		}
		return fn(cb)
	}
	if !mayCarry(m.Descriptor()) {
		return nil
	}
	var err error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					err = visit(mv.Message(), fn)
					return err == nil
				})
			}
		case fd.IsList():
			if fd.Message() != nil {
				for i, list := 0, v.List(); i < list.Len() && err == nil; i++ {
					err = visit(list.Get(i).Message(), fn)
				}
			}
		case fd.Message() != nil:
			err = visit(v.Message(), fn)
		}
		return err == nil
	})
	return err
}

// carriers caches, per message type, whether a Callback can appear anywhere inside it, so
// the requests that cannot carry one (nearly all) are not walked.
var carriers sync.Map // protoreflect.FullName -> bool

func mayCarry(md protoreflect.MessageDescriptor) bool {
	if v, ok := carriers.Load(md.FullName()); ok {
		return v.(bool)
	}
	r := reaches(md, map[protoreflect.FullName]bool{})
	carriers.Store(md.FullName(), r)
	return r
}

func reaches(md protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if md.FullName() == callbackName {
		return true
	}
	if seen[md.FullName()] {
		return false
	}
	seen[md.FullName()] = true
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		sub := fd.Message()
		if fd.IsMap() {
			sub = fd.MapValue().Message()
		}
		if sub != nil && reaches(sub, seen) {
			return true
		}
	}
	return false
}
