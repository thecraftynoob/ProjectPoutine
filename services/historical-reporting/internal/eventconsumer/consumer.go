// Package eventconsumer is Historical Reporting's durable NATS JetStream
// ingestion side: it subscribes to Task Router's full published event
// catalog (all three domains -- task, agent, reservation) and materializes
// every message into pgstore.Store's generic historical_events table.
//
// Task Router's own stream/subject constants
// (services/task-router/internal/events.StreamName / .StreamSubjects) are
// deliberately NOT imported here, even though this is a same-module,
// same-repo build. Two independent reasons, either one sufficient on its
// own:
//
//  1. Go language rule: internal/events is an internal package of a
//     DIFFERENT service (services/task-router/...), and Go's internal
//     package visibility only permits imports from within
//     services/task-router/ itself -- this package physically cannot
//     import it.
//  2. CLAUDE.md Rule 3 (strict domain boundaries): even if Go somehow
//     allowed it, importing another service's internal Go types would be
//     exactly the kind of implementation coupling Rule 3 forbids.
//     Cross-service coordination in this repo happens via the event bus's
//     subject-naming CONVENTION -- "tenant.{tenant_id}.{domain}.{event_type}"
//     and the well-known "TASK_ROUTER_EVENTS" stream name -- which IS the
//     public contract, not via sharing Go source.
//
// So this package defines its own local copies of the stream name and
// subject filters it needs (see streamName/streamSubjects/subjectFilter
// below), documented as Task Router's public NATS contract rather than an
// implementation-coupled import.
//
// Delivery guarantee / known gap: Subscribe uses a DURABLE, fixed-name
// JetStream consumer (pkg/eventbus.Client.Subscribe, not
// SubscribeEphemeral -- see that method's doc comment for why: Historical
// Reporting is a single-replica, stateful-ingestion service that must
// resume exactly where it left off after a restart, not a stateless
// fan-out replica). JetStream's delivery guarantee under AckExplicitPolicy
// is at-least-once, not exactly-once: if this process crashes after
// inserting a row but before acking the message, JetStream will redeliver
// that message on reconnect, and since event_id is generated fresh at
// insertion time (there is no stable application-level event ID on the
// wire to dedupe against -- Task Router's structpb payloads carry no UUID
// field of their own, see services/task-router/internal/events/events.go),
// a redelivery after a crash-before-ack window can double-insert a row as
// two distinct event_id values for what was really one event. This is an
// accepted, documented milestone-scope gap, not a silently swallowed one
// -- mirroring how services/api-gateway/internal/wsticket documents its
// own short-TTL-not-single-use tradeoff instead of pretending it doesn't
// exist. A future milestone wanting exactly-once semantics would need
// either a stable event ID added to the publish side (out of scope here --
// no publisher changes permitted this pass) or a dedupe strategy keyed on
// something derivable purely from the message itself (e.g. NATS JetStream
// message sequence number, persisted alongside the row).
package eventconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/services/historical-reporting/internal/pgstore"
)

// streamName is Task Router's own JetStream stream name
// (services/task-router/internal/events.StreamName) -- reproduced here as
// this package's own constant rather than imported. See package doc
// comment for why.
const streamName = "TASK_ROUTER_EVENTS"

// consumerName is the fixed, stable durable consumer name this service
// uses across restarts. Durability depends on this never changing --
// JetStream tracks delivery position server-side keyed by this exact
// name, so renaming it would silently start a brand-new consumer with no
// memory of what was already delivered (effectively a full stream
// replay), not a graceful rename.
const consumerName = "historical-reporting-ingest"

// subjectFilter covers all three of Task Router's published domains
// (task/agent/reservation) in a single JetStream consumer, using the
// wildcard form "tenant.*.>" rather than three separate Subscribe calls.
//
// This was a genuine technical unknown going in, not just a style
// choice: JetStream consumer FilterSubject supports the same wildcard
// syntax as stream subjects (">" matches one-or-more trailing tokens),
// and testing confirms a single durable consumer's FilterSubject of
// "tenant.*.>" against a stream registered with subjects
// ["tenant.*.task.>", "tenant.*.agent.>", "tenant.*.reservation.>"]
// receives messages published to all three -- see consumer_test.go's
// TestConsumer_ReceivesAllThreeDomainsOnOneSubscribeCall, which publishes
// one event per domain and asserts a single Subscribe call's handler
// receives all three. One Subscribe call, one durable consumer, is
// therefore sufficient; three separate FilterSubject-scoped Subscribe
// calls (which would also require three independent consumer names) are
// unnecessary here.
const subjectFilter = "tenant.*.>"

// Consumer wires a durable eventbus.Client.Subscribe to pgstore.Store,
// materializing every received message into historical_events.
type Consumer struct {
	client *eventbus.Client
	store  *pgstore.Store
	logger *slog.Logger
}

// NewConsumer constructs a Consumer.
func NewConsumer(client *eventbus.Client, store *pgstore.Store, logger *slog.Logger) *Consumer {
	return &Consumer{client: client, store: store, logger: logger}
}

// EnsureStream defensively provisions the TASK_ROUTER_EVENTS stream with
// Task Router's own subject set, using the same local-copy constants this
// package already needs for subjectFilter.
//
// Why Historical Reporting calls this at all, given Task Router already
// calls its own equivalent (events.Publisher.EnsureStream) at its own
// startup: JetStream's CreateOrUpdateStream is documented as idempotent
// and safe to call redundantly from multiple services against the same
// stream name/subjects (it's a declarative "ensure this exists with this
// config" call, not a create-or-fail one) -- and CreateOrUpdateConsumer
// (what Subscribe calls internally) requires the target stream to
// already exist. Since this repo's services can start in any order (esp.
// in K8s, where pod scheduling order isn't guaranteed, and in local dev,
// where an operator might reasonably run historical-reporting before ever
// running task-router), Historical Reporting cannot assume Task Router
// has already created the stream by the time it starts. Calling
// EnsureStream here removes that ordering dependency entirely, at the
// cost of this package needing to know Task Router's subject list too
// (already true for subjectFilter above, so no new coupling is
// introduced).
func (c *Consumer) EnsureStream(ctx context.Context) error {
	return c.client.EnsureStream(ctx, streamName, []string{
		"tenant.*.task.>",
		"tenant.*.agent.>",
		"tenant.*.reservation.>",
	})
}

// Start subscribes via a durable, fixed-name JetStream consumer
// (consumerName) covering subjectFilter, and materializes every received
// message into historical_events until ctx is cancelled. Returns once the
// underlying Subscribe call has registered the consumer and started
// pulling (matching pkg/eventbus.Client.Subscribe's own
// register-then-return-immediately contract -- delivery continues in the
// background until ctx is done).
func (c *Consumer) Start(ctx context.Context) error {
	return c.client.Subscribe(ctx, streamName, consumerName, subjectFilter, c.handle)
}

// handle materializes one JetStream message into historical_events.
// Returning a non-nil error causes pkg/eventbus's shared consume loop to
// Nak the message (leaving it for redelivery); returning nil causes it to
// Ack. This function never calls Ack/Nak itself -- see
// pkg/eventbus.Client.consume's doc comment for why that's the shared
// loop's job, not each handler's.
func (c *Consumer) handle(msg jetstream.Msg) error {
	subject := msg.Subject()

	parsed, err := parseSubject(subject)
	if err != nil {
		c.logger.Error("failed to parse event subject, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("eventconsumer: parse subject %q: %w", subject, err)
	}

	payloadJSON, err := protoStructToJSON(msg.Data())
	if err != nil {
		c.logger.Error("failed to decode event payload, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("eventconsumer: decode payload for %q: %w", subject, err)
	}

	ctx := context.Background()
	eventID, err := c.store.InsertEvent(ctx, parsed.TenantID, parsed.Domain, parsed.EventType, subject, payloadJSON)
	if err != nil {
		c.logger.Error("failed to insert historical event, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("eventconsumer: insert event for %q: %w", subject, err)
	}

	c.logger.Info("ingested event",
		slog.String("event_id", eventID.String()),
		slog.String("tenant_id", parsed.TenantID.String()),
		slog.String("domain", parsed.Domain),
		slog.String("event_type", parsed.EventType),
	)
	return nil
}

// protoStructToJSON unmarshals a protobuf-encoded structpb.Struct (the
// wire format every pkg/eventbus.Client.PublishEvent call uses -- see
// services/task-router/internal/events.structFrom) and re-marshals it as
// plain JSON suitable for a JSONB column, via structpb.Struct's own
// MarshalJSON (which correctly follows the protobuf-JSON canonical
// mapping, including nested structs/lists/nulls) rather than hand-rolling
// a Struct->map->json.Marshal conversion.
func protoStructToJSON(data []byte) (json.RawMessage, error) {
	var s structpb.Struct
	if err := proto.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("eventconsumer: unmarshal protobuf struct: %w", err)
	}
	b, err := s.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("eventconsumer: marshal struct as json: %w", err)
	}
	return json.RawMessage(b), nil
}
