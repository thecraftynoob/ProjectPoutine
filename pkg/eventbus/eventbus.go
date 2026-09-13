// Package eventbus wraps github.com/nats-io/nats.go (JetStream) to
// implement the event-bus conventions from CCAAS_ENTERPRISE_ARCHITECTURE.md
// Sections 1.2, 3.1, and 3.2:
//
//   - Every event subject is namespaced tenant.{tenant_id}.{domain}.{event_type}.
//   - Domain events are published to durable, replayable JetStream streams,
//     never fire-and-forget core NATS subjects.
//   - Consumers are durable pull consumers, supporting multiple independent
//     consumer groups (each with its own full copy of the stream) and
//     competing consumers within a group (see Section 6.4 of
//     TASK_ROUTER_SPECIFICATION.md for the delivery-guarantee shape this
//     mirrors).
package eventbus

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

// Subject builds the tenant-scoped event subject
// "tenant.{tenant_id}.{domain}.{event_type}" per architecture doc
// Section 2.2/5 naming convention (e.g. "tenant.acme.voice.call.ended").
func Subject(tenantID uuid.UUID, domain, eventType string) string {
	return fmt.Sprintf("tenant.%s.%s.%s", tenantID.String(), domain, eventType)
}

// Client wraps a NATS connection and its JetStream context, using the
// newer github.com/nats-io/nats.go/jetstream API (not the legacy
// JetStreamContext API).
type Client struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// Connect establishes a NATS connection and JetStream context against the
// given URL (e.g. "nats://localhost:4222").
func Connect(url string) (*Client, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("eventbus: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("eventbus: create jetstream context: %w", err)
	}
	return &Client{conn: nc, js: js}, nil
}

// Close drains and closes the underlying NATS connection.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// EnsureStream idempotently creates or updates a JetStream stream named
// `name` covering the given subject filters (e.g. ["tenant.*.voice.>"]).
// Safe to call on every service startup.
func (c *Client) EnsureStream(ctx context.Context, name string, subjects []string) error {
	_, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		return fmt.Errorf("eventbus: ensure stream %q: %w", name, err)
	}
	return nil
}

// PublishEvent marshals payload as protobuf and publishes it to the
// tenant-scoped subject built from (tenantID, domain, eventType), via
// JetStream so the publish is durable and acknowledged by the stream.
func (c *Client) PublishEvent(ctx context.Context, tenantID uuid.UUID, domain, eventType string, payload proto.Message) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("eventbus: marshal payload: %w", err)
	}
	subject := Subject(tenantID, domain, eventType)
	if _, err := c.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("eventbus: publish to %q: %w", subject, err)
	}
	return nil
}

// Subscribe creates (or reuses) a durable pull consumer named
// consumerName on stream streamName, filtered to subjectFilter, and runs
// handler for every delivered message until ctx is cancelled.
//
// Multiple independent calls with different consumerName values on the
// same stream form independent consumer groups (each sees every message).
// Multiple processes calling Subscribe with the SAME consumerName on the
// same stream are competing consumers within one group (each message is
// delivered to exactly one of them) — this is how the matching engine /
// worker pool style horizontal scaling described in the architecture doc
// is achieved.
func (c *Client) Subscribe(ctx context.Context, streamName, consumerName, subjectFilter string, handler func(msg jetstream.Msg) error) error {
	cons, err := c.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subjectFilter,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("eventbus: create consumer %q on stream %q: %w", consumerName, streamName, err)
	}

	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		if err := handler(msg); err != nil {
			// Leave it to redelivery; a future milestone may add structured
			// logging/backoff/dead-lettering here.
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("eventbus: consume on %q: %w", consumerName, err)
	}

	go func() {
		<-ctx.Done()
		consCtx.Stop()
	}()

	return nil
}
