package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
)

// StreamName is Task Router's JetStream stream name (see
// services/task-router/internal/events.StreamName). Duplicated here as a
// literal, rather than importing Task Router's internal/events package,
// since internal packages are not importable across module-internal
// service boundaries in Go and this service must not depend on Task
// Router's implementation package -- only on the wire contract (subject
// names and payload shapes), which is what subjects.go / envelope.go
// encode against.
const StreamName = "TASK_ROUTER_EVENTS"

// consumerInactiveThreshold bounds how long an ephemeral consumer this
// service creates (see Start, below) is allowed to sit idle -- no active
// Consume pull -- before JetStream deletes it server-side. This is what
// keeps consumers from accumulating unboundedly across restarts/crashes
// of this process without requiring an explicit delete-on-shutdown call:
// a consumer this replica stops pulling from (clean shutdown, crash, or
// otherwise) is reaped automatically well within a typical redeploy
// cadence.
const consumerInactiveThreshold = 5 * time.Minute

// Consumer subscribes to the four forwarded Task Router event subjects
// (per TASK_ROUTER_SPECIFICATION.md Section 6.3's filtered subset) via an
// ephemeral JetStream pull consumer for each (see Start's doc comment for
// why ephemeral, not durable-and-shared), decodes each event, and
// forwards it through a Fanout so every replica's local Registry gets a
// chance to deliver it to a connected client.
type Consumer struct {
	bus    *eventbus.Client
	fanout *Fanout
	logger *slog.Logger

	mu          sync.Mutex
	seenTenants map[string]bool
}

// NewConsumer constructs a Consumer. Call Start once at service startup.
func NewConsumer(bus *eventbus.Client, fanout *Fanout, logger *slog.Logger) *Consumer {
	return &Consumer{
		bus:         bus,
		fanout:      fanout,
		logger:      logger,
		seenTenants: make(map[string]bool),
	}
}

// Start creates one ephemeral JetStream consumer per forwarded subject
// filter and begins processing. It returns once all subscriptions are
// established (matching pkg/eventbus.Client.SubscribeEphemeral's own
// semantics of running the handler in a background goroutine
// internally); processing continues until ctx is cancelled.
//
// Ephemeral consumers (via SubscribeEphemeral), not durable ones sharing
// a deterministic name (the previous, buggy behavior of this method) are
// required here: this service's cross-replica delivery design (see
// fanout.go) depends on EVERY replica's own consumer independently
// receiving every forwarded event, so it can then correctly decide, via
// the Redis fan-out, whether it locally owns the target connection. A
// durable consumer name that is identical across replicas (as
// "agent-presence-relay-" + a sanitized subject necessarily is, since
// every replica runs the same binary) would instead make every replica
// share ONE JetStream consumer group, turning them into competing
// consumers -- each event delivered to exactly one replica's process,
// never all of them -- which silently breaks fan-out delivery for
// roughly (N-1)/N of events once this service is scaled past one
// replica, without any error or symptom at replicas=1. An ephemeral
// consumer is unnamed and unshared by construction, so every replica
// (including multiple Start calls even within one process, e.g. in
// tests) always gets its own independent full copy of the stream.
func (c *Consumer) Start(ctx context.Context) error {
	for _, filter := range ForwardedSubjectFilters {
		filter := filter
		err := c.bus.SubscribeEphemeral(ctx, StreamName, filter, consumerInactiveThreshold, func(msg jetstream.Msg) error {
			return c.handle(ctx, msg)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// handle processes a single delivered JetStream message: classify its
// subject, decode its payload, ensure this replica is subscribed to the
// event's tenant's Redis fan-out channel, build the client Envelope, and
// publish it for delivery.
func (c *Consumer) handle(ctx context.Context, msg jetstream.Msg) error {
	subject := msg.Subject()
	eventType, tenantID, ok := classify(subject)
	if !ok {
		// Should not happen given ForwardedSubjectFilters, but if the
		// stream ever redelivers something unexpected, drop it rather
		// than forward an unclassified event to any client.
		c.logger.Warn("relay: received message on unexpected subject, dropping", slog.String("subject", subject))
		return nil
	}

	fields, err := decodePayload(msg.Data())
	if err != nil {
		c.logger.Error("relay: failed to decode event payload", slog.String("subject", subject), slog.Any("error", err))
		// Malformed payload from our own upstream is not something
		// redelivery will fix; ack (return nil) rather than let it Nak
		// forever. Logged for operator visibility.
		return nil
	}

	env, err := buildEnvelope(eventType, fields)
	if err != nil {
		c.logger.Error("relay: failed to build envelope", slog.String("subject", subject), slog.Any("error", err))
		return nil
	}

	c.ensureSubscribed(ctx, tenantID)

	if err := c.fanout.Publish(ctx, tenantID, env); err != nil {
		c.logger.Error("relay: failed to publish envelope to fanout", slog.String("subject", subject), slog.Any("error", err))
		return err
	}
	return nil
}

// ensureSubscribed lazily starts this replica's Redis Pub/Sub
// subscription for tenantID the first time an event for that tenant is
// observed, so the service does not need a separate "list all tenants"
// bootstrap step -- tenants are discovered organically from the event
// stream itself, consistent with there being no tenant-provisioning
// hook available to this service (see agent-presence README).
func (c *Consumer) ensureSubscribed(ctx context.Context, tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seenTenants[tenantID] {
		return
	}
	c.seenTenants[tenantID] = true
	c.fanout.Subscribe(ctx, tenantID)
}
