package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/thecraftynoob/ProjectPoutine/services/agent-presence/internal/registry"
)

// Cross-replica fan-out design.
//
// Agent & Presence Service's live WebSocket registry (internal/registry)
// is deliberately per-process, in-memory state (architecture doc Section
// 2.2 lists this service as "Stateful (live WebSocket registry)"). With
// more than one replica running, the NATS consumer instance that
// receives a given Task Router event has no way to know, up front, which
// replica (if any) is holding that event's target agent's WebSocket
// connection.
//
// Rather than branching on "check local registry first, and only if
// absent fall back to a cross-replica lookup" (two code paths, and the
// awkward question of how a replica would even ask another replica "do
// you have agent X" synchronously), this package uses ONE code path for
// every event, whether or not the connection turns out to be local:
//
//  1. On receipt of a forwarded NATS event, build the client Envelope
//     and publish it as JSON to a per-tenant Redis Pub/Sub channel
//     (agent-presence:tenant:{tenantId}:events) -- fire-and-forget, all
//     replicas subscribe to the same channel.
//  2. Every replica (including the one that received the NATS message)
//     is independently subscribed to every tenant channel it has ever
//     seen traffic for (subscriptions are created lazily, per tenant,
//     the first time that tenant is observed) and, on receiving a
//     fanned-out message, checks its OWN local Registry via Lookup. If
//     it owns the target agent's connection, it delivers the message;
//     if not, it silently discards it.
//
// This means exactly one replica actually writes to a WebSocket for any
// given event, every replica does identical, simple work (subscribe,
// look up locally, maybe deliver), and there is no "ask around the
// cluster" round trip or coordination protocol needed. The cost is that
// every replica receives every tenant's fanned-out events regardless of
// whether it holds a matching connection -- an acceptable trade for a
// home-lab-scale deployment, and consistent with this being an explicit,
// specified requirement (architecture doc Section 2.2's Key Dependencies
// column: "Redis (pub/sub fan-out across replicas ...)"), not a
// hypothetical extension.
//
// Redis is used purely as an ephemeral pub/sub transport here -- nothing
// is persisted to Redis by this package (contrast with the "presence key
// storage with TTL heartbeats" half of that same architecture-doc
// column, which is a real gap: this milestone implements the pub/sub
// fan-out half only, since a durable presence-key registry is not needed
// by anything in this milestone's scope -- see the service README's
// Deferred section).

// Fanout owns the Redis publish/subscribe side of cross-replica event
// delivery. One Fanout is created per agent-presence process and shared
// between the NATS-consuming relay.Consumer and the local Registry.
type Fanout struct {
	redis    *redis.Client
	registry *registry.Registry
	logger   *slog.Logger
}

// NewFanout constructs a Fanout backed by an already-connected Redis
// client and the process's local connection Registry.
func NewFanout(redisClient *redis.Client, reg *registry.Registry, logger *slog.Logger) *Fanout {
	return &Fanout{redis: redisClient, registry: reg, logger: logger}
}

// Publish fans an Envelope out to every replica subscribed to the given
// tenant's channel (including, harmlessly, this same process). Errors
// are logged and swallowed by callers per the same "best-effort
// downstream notification" philosophy Task Router's own event publisher
// uses (internal/events doc comment) -- a Redis publish failure must
// never be allowed to affect any domain state, and there is none to
// affect here.
func (f *Fanout) Publish(ctx context.Context, tenantID string, env Envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("relay: marshal envelope for fanout: %w", err)
	}
	if err := f.redis.Publish(ctx, channelName(tenantID), data).Err(); err != nil {
		return fmt.Errorf("relay: publish to redis channel: %w", err)
	}
	return nil
}

// Subscribe starts a background goroutine that listens on tenantID's
// fan-out channel and delivers any message whose target agent has a
// connection registered locally. It returns immediately; the goroutine
// runs until ctx is cancelled. Safe to call multiple times for the same
// tenantID -- go-redis's PubSub is per-call, so each call opens its own
// subscription (harmless duplication for this milestone's scale; a
// future optimization could de-duplicate by tenant, noted here rather
// than built preemptively).
func (f *Fanout) Subscribe(ctx context.Context, tenantID string) {
	sub := f.redis.Subscribe(ctx, channelName(tenantID))
	ch := sub.Channel()

	go func() {
		defer sub.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				f.deliverIfLocal(tenantID, msg.Payload)
			}
		}
	}()
}

// deliverIfLocal decodes one fanned-out message and, if this replica
// holds a connection for the envelope's target agent, writes it to that
// connection.
func (f *Fanout) deliverIfLocal(tenantID, payload string) {
	var env Envelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		f.logger.Error("relay: failed to decode fanned-out envelope", slog.Any("error", err))
		return
	}
	conn, ok := f.registry.Lookup(tenantID, env.AgentID)
	if !ok {
		// Not ours -- some other replica (or no replica at all, if the
		// agent isn't currently connected anywhere) owns this delivery.
		return
	}
	data, err := json.Marshal(env)
	if err != nil {
		f.logger.Error("relay: failed to re-marshal envelope for delivery", slog.Any("error", err))
		return
	}
	if err := conn.Send(data); err != nil {
		f.logger.Warn("relay: failed to deliver event to local connection",
			slog.String("tenant_id", tenantID),
			slog.String("agent_id", env.AgentID),
			slog.Any("error", err))
	}
}
