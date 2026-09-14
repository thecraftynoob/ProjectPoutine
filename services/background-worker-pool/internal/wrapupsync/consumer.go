// Package wrapupsync implements Background Worker Pool's first real
// milestone end to end: a durable NATS JetStream consumer that turns Task
// Router's task.completed event into a wrapup_sync background_jobs row
// (Consumer, this file), plus the pgqueue.Poller handler that later
// claims that row and does a real outbound HTTP POST to a
// tenant-configured URL (see handler.go).
//
// Task Router's own stream/subject constants
// (services/task-router/internal/events.StreamName / .StreamSubjects) are
// deliberately NOT imported here, for the same two reasons
// services/historical-reporting/internal/eventconsumer's package doc
// comment already documents for this repo's convention: (1) Go's
// internal-package visibility forbids importing a different service's
// internal/ tree, and (2) CLAUDE.md Rule 3 forbids the implementation
// coupling even if Go allowed it. So this package defines its own local
// copies of the stream name and the ONE subject filter it needs.
//
// Unlike Historical Reporting, this service does not want the full event
// catalog -- it only cares about task.completed ("every completed task
// must eventually get a wrap-up job"), so subjectFilter below is scoped
// to "tenant.*.task.completed", not a wildcard covering every domain.
//
// Delivery guarantee: Subscribe uses a DURABLE, fixed-name JetStream
// consumer (pkg/eventbus.Client.Subscribe, not SubscribeEphemeral) --
// confirmed with the user this milestone's guarantee is real, not
// best-effort ("every completed task must eventually get a wrap-up job"),
// the same durability reasoning Historical Reporting's consumer already
// established for this repo. A crash between pgqueue.Enqueue succeeding
// and the message being acked causes JetStream to redeliver the message,
// resulting in a second, duplicate wrapup_sync job for the same
// task.completed event -- the same at-least-once/no-dedupe tradeoff
// Historical Reporting's package doc comment documents, not a new one
// this package invents. See GAPS.md's existing idempotency-key gap entry
// (PROGRESS.md To-Do #9b) -- this is the same underlying gap (Task
// Router's published payloads carry no stable event ID), surfacing again
// in a second consumer.
package wrapupsync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thecraftynoob/ProjectPoutine/pkg/eventbus"
	"github.com/thecraftynoob/ProjectPoutine/pkg/pgqueue"
)

// streamName is Task Router's own JetStream stream name
// (services/task-router/internal/events.StreamName) -- reproduced here as
// this package's own constant rather than imported. See package doc
// comment for why.
const streamName = "TASK_ROUTER_EVENTS"

// consumerName is the fixed, stable durable consumer name this service
// uses across restarts. Must never change once chosen -- JetStream tracks
// delivery position server-side keyed by this exact name, so renaming it
// would silently start a brand-new consumer with no memory of what was
// already delivered.
const consumerName = "background-worker-pool-wrapup"

// subjectFilter covers ONLY task.completed, across every tenant --
// deliberately narrower than Historical Reporting's "tenant.*.>": this
// service only cares about this one event type, not Task Router's full
// catalog.
const subjectFilter = "tenant.*.task.completed"

// JobType is the background_jobs.job_type value this consumer enqueues
// and the poller handler in handler.go dispatches on.
const JobType = "wrapup_sync"

// WrapupSyncPayload is the JSON shape stored in background_jobs.payload
// for every wrapup_sync job -- the fields a wrap-up sync POST body needs.
// AgentID is a plain string, not uuid.UUID, because Task Router's
// TaskCompleted event publishes it as nullable/possibly-empty (see
// services/task-router/internal/events/events.go's TaskCompleted doc
// comment: "agentId (nullable)") -- a task can complete with no agent
// ever having been assigned.
type WrapupSyncPayload struct {
	TaskID   string    `json:"taskId"`
	AgentID  string    `json:"agentId,omitempty"`
	TenantID uuid.UUID `json:"tenantId"`
}

// Consumer wires a durable eventbus.Client.Subscribe to pgqueue.Enqueue,
// turning every received task.completed event into one wrapup_sync
// background_jobs row.
type Consumer struct {
	client *eventbus.Client
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewConsumer constructs a Consumer.
func NewConsumer(client *eventbus.Client, pool *pgxpool.Pool, logger *slog.Logger) *Consumer {
	return &Consumer{client: client, pool: pool, logger: logger}
}

// EnsureStream defensively provisions the TASK_ROUTER_EVENTS stream with
// Task Router's own subject set, mirroring Historical Reporting's
// Consumer.EnsureStream exactly: Task Router might not have started yet
// (K8s pod scheduling order isn't guaranteed, and an operator might
// reasonably run this service before ever running task-router in local
// dev), and CreateOrUpdateStream is documented as idempotent/safe to call
// redundantly, so this removes any startup-order dependency.
func (c *Consumer) EnsureStream(ctx context.Context) error {
	return c.client.EnsureStream(ctx, streamName, []string{
		"tenant.*.task.>",
		"tenant.*.agent.>",
		"tenant.*.reservation.>",
	})
}

// Start subscribes via a durable, fixed-name JetStream consumer
// (consumerName) covering subjectFilter, and enqueues a wrapup_sync job
// for every received message until ctx is cancelled. Returns once the
// underlying Subscribe call has registered the consumer and started
// pulling -- delivery continues in the background until ctx is done.
func (c *Consumer) Start(ctx context.Context) error {
	return c.client.Subscribe(ctx, streamName, consumerName, subjectFilter, c.handle)
}

// handle parses one task.completed message and enqueues a wrapup_sync
// job. Returning a non-nil error causes pkg/eventbus's shared consume
// loop to Nak the message (leaving it for redelivery); returning nil
// causes it to Ack. This function never calls Ack/Nak itself -- see
// pkg/eventbus.Client.consume's doc comment for why that's the shared
// loop's job, not each handler's.
func (c *Consumer) handle(msg jetstream.Msg) error {
	subject := msg.Subject()

	parsed, err := parseSubject(subject)
	if err != nil {
		c.logger.Error("failed to parse event subject, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("wrapupsync: parse subject %q: %w", subject, err)
	}

	var payload structpb.Struct
	if err := proto.Unmarshal(msg.Data(), &payload); err != nil {
		c.logger.Error("failed to decode event payload, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("wrapupsync: decode payload for %q: %w", subject, err)
	}
	fields := payload.GetFields()

	taskID := fields["taskId"].GetStringValue()
	if taskID == "" {
		c.logger.Error("task.completed event missing taskId, nak'ing for redelivery", slog.String("subject", subject))
		return fmt.Errorf("wrapupsync: event on %q has no taskId field", subject)
	}
	// agentId is nullable per events.TaskCompleted's doc comment -- a
	// null structpb value's GetStringValue() correctly returns "".
	agentID := fields["agentId"].GetStringValue()

	jobPayload := WrapupSyncPayload{
		TaskID:   taskID,
		AgentID:  agentID,
		TenantID: parsed.TenantID,
	}
	payloadJSON, err := json.Marshal(jobPayload)
	if err != nil {
		c.logger.Error("failed to marshal wrapup_sync job payload, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("wrapupsync: marshal job payload: %w", err)
	}

	ctx := context.Background()
	if err := pgqueue.Enqueue(ctx, c.pool, parsed.TenantID, JobType, payloadJSON, time.Now()); err != nil {
		c.logger.Error("failed to enqueue wrapup_sync job, nak'ing for redelivery", slog.String("subject", subject), slog.Any("error", err))
		return fmt.Errorf("wrapupsync: enqueue job: %w", err)
	}

	c.logger.Info("enqueued wrapup_sync job",
		slog.String("tenant_id", parsed.TenantID.String()),
		slog.String("task_id", taskID),
		slog.String("agent_id", agentID),
	)
	return nil
}
