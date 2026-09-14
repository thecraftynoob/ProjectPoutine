// Package internal holds domain logic for background-worker-pool. See
// internal/wrapupsync for this service's first real milestone (durable
// task.completed consumption -> pgqueue.Poller -> outbound HTTP wrap-up
// sync) and internal/pgstore for its Postgres persistence layer.
package internal
