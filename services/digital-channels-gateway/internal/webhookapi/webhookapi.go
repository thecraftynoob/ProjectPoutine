// Package webhookapi implements Digital Channels Gateway's first real
// domain milestone: an inbound webhook endpoint for one generic "chat"
// channel shape, normalizing an inbound message into Task Router's
// generic Task abstraction via EnqueueTask.
//
// Scope (deliberately narrow, per architecture doc Section 2.2's
// "normalizing inbound Chat/SMS/Email/Social into Task Router's generic
// Task abstraction" and its explicit deferral of message/thread
// persistence to future async workers, not inline):
//   - Inbound only. No outbound/agent-reply delivery back to the channel.
//   - One generic channel shape ("chat"), not a real per-provider
//     integration (Twilio, etc.).
//   - No Postgres persistence of the inbound message/session -- nothing in
//     this package writes to any database. session_id travels through
//     purely for idempotency/correlation by a future milestone; today it
//     is validated for presence and otherwise unused.
//   - Ends at a successfully enqueued Task Router Task. Nothing about
//     matching, reservation, or delivery is this package's concern.
package webhookapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	taskrouterv1 "github.com/thecraftynoob/ProjectPoutine/pkg/genproto/task-router/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chatTaskType is the fixed Task Router task_type stamped on every task
// created by this milestone's single generic "chat" webhook shape. A real
// multi-channel build would derive this from the actual provider/channel
// (sms, email, chat, social, ...); fixing it here is an explicit,
// documented simplification matching the milestone's "one generic chat
// webhook shape" scope, not an oversight.
const chatTaskType = "chat"

// InboundChatMessage is the generic JSON body this milestone's webhook
// accepts, standing in for what a real per-provider adapter (Twilio,
// etc.) would normalize into before this package ever sees it.
//
// SessionID identifies the conversation for idempotency/correlation.
// Nothing in this milestone persists or deduplicates on it yet (message/
// thread persistence is explicitly deferred to a future async-worker
// milestone, architecture doc Section 2.2) -- it is validated as required
// so the wire shape is already correct for when that lands, and so a
// caller integrating against this endpoint today builds the right habit.
//
// QueueID lets the test caller pick which Task Router queue to route
// into, since no real routing-rules engine exists yet in this milestone --
// it must reference a queue that already exists in the target tenant's
// Task Router Queue registry, or EnqueueTask rejects the request.
type InboundChatMessage struct {
	SessionID  string                    `json:"session_id"`
	From       string                    `json:"from"`
	Text       string                    `json:"text"`
	QueueID    string                    `json:"queue_id"`
	Attributes map[string]AttributeValue `json:"attributes,omitempty"`
}

// AttributeValue is the wire shape for one entry of InboundChatMessage's
// optional Attributes map, mirroring Task Router's own AttributeValue
// oneof (proto/task-router/v1/task_router.proto): exactly one of
// NumberValue/BoolValue is expected to be set on a valid entry. JSON has
// no native discriminated-union shape, so this is expressed as two
// optional pointer fields rather than a oneof -- IsBool being explicitly
// true (not just BoolValue's presence) is what selects the boolean arm,
// matching services/task-router/internal/grpcapi/convert.go's own
// IsBool-discriminated AttributeValue convention on the domain side.
type AttributeValue struct {
	IsBool      bool    `json:"is_bool,omitempty"`
	BoolValue   bool    `json:"bool_value,omitempty"`
	NumberValue float64 `json:"number_value,omitempty"`
}

// errorResponse is this package's JSON error body shape, mirroring
// services/api-gateway/internal/gwauth's convention of a flat
// {"error": "..."} object for every HTTP error response.
type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func attrsToProto(in map[string]AttributeValue) map[string]*taskrouterv1.AttributeValue {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*taskrouterv1.AttributeValue, len(in))
	for k, v := range in {
		if v.IsBool {
			out[k] = &taskrouterv1.AttributeValue{Kind: &taskrouterv1.AttributeValue_BoolValue{BoolValue: v.BoolValue}}
		} else {
			out[k] = &taskrouterv1.AttributeValue{Kind: &taskrouterv1.AttributeValue_NumberValue{NumberValue: v.NumberValue}}
		}
	}
	return out
}

// taskResponse is the JSON shape returned on success: the created Task
// Router Task, in the same field naming grpc-gateway/protojson would use
// elsewhere in this repo (camelCase), so a caller who already consumes
// API Gateway's REST surface sees a familiar shape here too.
type taskResponse struct {
	TaskID   string `json:"taskId"`
	QueueID  string `json:"queueId"`
	TaskType string `json:"taskType"`
	Status   string `json:"status"`
}

// Handler serves POST /webhooks/chat/{tenant_id}.
type Handler struct {
	// EnqueueTask calls Task Router's EnqueueTask RPC as a service
	// (pkg/svcauth-authenticated), scoped to the tenant_id in the
	// request path. cmd/main.go wires this to a real per-tenant-scoped
	// call (minting a fresh service token per tenant via
	// svcauth.TokenSource, since the tenant is only known once the
	// webhook path is parsed, not at process-startup time); tests
	// supply a fake.
	EnqueueTask func(ctx context.Context, tenantID string, req *taskrouterv1.EnqueueTaskRequest) (*taskrouterv1.Task, error)
	Logger      *slog.Logger
}

func (h *Handler) logger() *slog.Logger {
	if h.Logger != nil {
		return h.Logger
	}
	return slog.Default()
}

// ServeHTTP implements POST /webhooks/chat/{tenant_id}.
//
// SECURITY TRADEOFF -- read before extending: this endpoint verifies NO
// signature or shared secret from the calling channel provider. tenant_id
// comes solely from the URL path, and the request body is trusted as-is.
// This is a deliberate, explicit scope decision for this milestone (there
// is no real provider account yet to verify a signature against), not an
// oversight -- mirroring how services/api-gateway/internal/wsticket
// documents its own short-TTL-not-single-use tradeoff instead of silently
// shipping it. A production build MUST add per-tenant webhook signature
// verification (e.g. an HMAC secret provisioned per tenant alongside its
// webhook URL, checked against a request header) before this endpoint is
// exposed to a real, untrusted, public-internet channel provider --
// anyone who discovers or guesses a tenant_id can otherwise enqueue tasks
// into that tenant's queues.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" {
		writeError(w, http.StatusNotFound, "webhookapi: tenant_id path segment is required")
		return
	}

	var msg InboundChatMessage
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&msg); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("webhookapi: invalid JSON body: %v", err))
		return
	}

	if missing := firstMissingField(msg); missing != "" {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("webhookapi: missing required field %q", missing))
		return
	}

	req := &taskrouterv1.EnqueueTaskRequest{
		QueueId:            msg.QueueID,
		TaskType:           chatTaskType,
		RequiredAttributes: attrsToProto(msg.Attributes),
	}

	task, err := h.EnqueueTask(r.Context(), tenantID, req)
	if err != nil {
		h.logger().Error("EnqueueTask failed",
			slog.String("tenant_id", tenantID),
			slog.String("queue_id", msg.QueueID),
			slog.String("session_id", msg.SessionID),
			slog.Any("error", err),
		)
		writeError(w, mapEnqueueError(err), publicEnqueueErrorMessage(err))
		return
	}

	writeJSON(w, http.StatusCreated, taskResponse{
		TaskID:   task.GetTaskId(),
		QueueID:  task.GetQueueId(),
		TaskType: task.GetTaskType(),
		Status:   task.GetStatus(),
	})
}

// firstMissingField returns the JSON field name of the first missing
// required field in msg (session_id, from, text, queue_id -- attributes
// is optional), or "" if all are present. Checked in a fixed field order
// so the error is deterministic.
func firstMissingField(msg InboundChatMessage) string {
	switch {
	case msg.SessionID == "":
		return "session_id"
	case msg.From == "":
		return "from"
	case msg.Text == "":
		return "text"
	case msg.QueueID == "":
		return "queue_id"
	default:
		return ""
	}
}

// mapEnqueueError maps an EnqueueTask gRPC error to an HTTP status.
// codes.InvalidArgument covers both genuinely malformed requests and
// Task Router's "queue does not exist" rejection (see
// services/task-router/internal/grpcapi/task.go's EnqueueTask -- a
// missing queue is InvalidArgument there, not NotFound, since queue_id is
// a request field, not a path-addressed resource on this RPC).
// codes.AlreadyExists (a colliding caller-supplied task_id -- not
// reachable via this webhook today since it never sets TaskId, but
// mapped defensively) is also a client-shaped error. Everything else
// (Internal, Unavailable, DeadlineExceeded, an unmapped/unknown error)
// is treated as an upstream failure and reported as 502.
func mapEnqueueError(err error) int {
	st, ok := status.FromError(err)
	if !ok {
		return http.StatusBadGateway
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.AlreadyExists:
		return http.StatusBadRequest
	default:
		return http.StatusBadGateway
	}
}

// publicEnqueueErrorMessage returns a caller-safe error message: the
// gRPC status message for the client-shaped cases mapEnqueueError treats
// as 400 (these are safe and useful to a webhook caller -- e.g. "queue
// %q does not exist"), and a generic message for everything else, so raw
// internal gRPC/transport error text is never leaked verbatim to an
// external caller. The full error is always logged server-side by the
// caller of this function (ServeHTTP, via slog) regardless.
func publicEnqueueErrorMessage(err error) string {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.InvalidArgument, codes.AlreadyExists:
			return "webhookapi: task router rejected the request: " + st.Message()
		}
	}
	return "webhookapi: failed to enqueue task with task router"
}
