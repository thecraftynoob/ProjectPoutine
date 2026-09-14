package redisdomain

import (
	"context"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

// ExpiredReservation is delivered to the caller-supplied handler in
// SubscribeExpiry for every reservation-expiry sentinel key TTL-expiry
// notification this replica observes.
type ExpiredReservation struct {
	TenantID      string
	ReservationID string
}

// SubscribeExpiry implements spec Section 4.3's expiry mechanism via the
// architectural decision recorded in the service README: Redis-native TTL
// + keyspace notifications, rather than a fixed-interval sweep. It
// subscribes to the "__keyevent@<db>__:expired" pub/sub channel (which
// requires the Redis instance to be configured with
// `notify-keyspace-events Ex` -- see docker-compose.yml and the service
// README) and, for every expired key matching the
// "tr:{tenant}:resexp:{reservationId}" shape produced by
// reservationExpiryKey, invokes handler with the parsed (tenantID,
// reservationID).
//
// Safe under multiple concurrent Task Router replicas: Redis keyspace
// notifications naturally fan out to every subscriber (this is normal
// Redis pub/sub, not a competing-consumer queue), so every replica
// observes every expiry notification and calls handler -- but handler is
// expected to resolve the reservation via RejectReservation, whose Lua
// script (reject_reservation.lua) atomically re-validates that the
// reservation is still "Offered" before applying anything (spec Section
// 5.4 rule 4). That atomic re-validation, not any locking here, is what
// prevents double-processing across replicas: only the first replica's
// call actually mutates anything; every other replica's call
// (redundantly, but harmlessly) observes CurrentStatus != Offered and
// no-ops.
//
// db is the Redis logical database index used by client (typically 0).
// Runs until ctx is cancelled.
func SubscribeExpiry(ctx context.Context, client *redis.Client, db int, logger *slog.Logger, handler func(ExpiredReservation)) error {
	if logger == nil {
		logger = slog.Default()
	}
	pubsub := client.PSubscribe(ctx, KeyspaceExpiryPattern(db))
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}

	ch := pubsub.Channel()
	go func() {
		<-ctx.Done()
		_ = pubsub.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			tenantID, reservationID, ok := ReservationIDFromExpiryKey(msg.Payload)
			if !ok {
				// Not a reservation-expiry key (e.g. an unrelated key in
				// the same Redis instance) -- ignore.
				continue
			}
			logger.Debug("reservation expiry notification received",
				slog.String("tenant_id", tenantID),
				slog.String("reservation_id", reservationID),
			)
			handler(ExpiredReservation{TenantID: tenantID, ReservationID: reservationID})
		}
	}
}

// FiredWrapUpTimer is delivered to the caller-supplied handler in
// SubscribeWrapUpTimeout for every wrap-up-timer sentinel key TTL-expiry
// notification this replica observes (Wrap Up / Disposition two-step
// completion lifecycle).
type FiredWrapUpTimer struct {
	TenantID string
	TaskID   string
}

// SubscribeWrapUpTimeout mirrors SubscribeExpiry exactly, but for the
// wrap-up timer (see keys.go's wrapUpExpiryKey/TaskIDFromWrapUpExpiryKey)
// instead of reservation expiry: same Redis-native TTL + keyspace
// notifications mechanism, same multi-replica safety story (every replica
// observes every expiry notification and calls handler, but handler is
// expected to resolve via (*Store).ResolveWrapUpTimeout, whose Lua script
// atomically re-validates the task is still WrapUp before applying
// anything -- only the first replica's call actually mutates anything;
// every other replica's call redundantly, but harmlessly, no-ops).
//
// db is the Redis logical database index used by client (typically 0).
// Runs until ctx is cancelled.
func SubscribeWrapUpTimeout(ctx context.Context, client *redis.Client, db int, logger *slog.Logger, handler func(FiredWrapUpTimer)) error {
	if logger == nil {
		logger = slog.Default()
	}
	pubsub := client.PSubscribe(ctx, KeyspaceExpiryPattern(db))
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		return err
	}

	ch := pubsub.Channel()
	go func() {
		<-ctx.Done()
		_ = pubsub.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			tenantID, taskID, ok := TaskIDFromWrapUpExpiryKey(msg.Payload)
			if !ok {
				// Not a wrap-up-timer key (e.g. a reservation-expiry key,
				// or an unrelated key, in the same Redis instance) --
				// ignore.
				continue
			}
			logger.Debug("wrap-up timer notification received",
				slog.String("tenant_id", tenantID),
				slog.String("task_id", taskID),
			)
			handler(FiredWrapUpTimer{TenantID: tenantID, TaskID: taskID})
		}
	}
}
