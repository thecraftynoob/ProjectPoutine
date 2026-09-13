package pgqueue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Poller is a small, reusable wrapper around the claim-then-dispatch loop
// sketched in architecture doc Section 3.3. It is intentionally generic
// (batch size, poll interval, and the handler are all caller-supplied)
// rather than a copy of the doc's illustrative sketch, so every service
// using the Database-as-a-Queue pattern (Background Worker Pool being the
// primary one) can reuse it instead of reimplementing the loop.
type Poller struct {
	Pool         *pgxpool.Pool
	BatchSize    int           // jobs claimed per poll; defaults to 20 if <= 0
	PollInterval time.Duration // how often to poll when idle; defaults to 2s if <= 0
	Logger       *slog.Logger  // defaults to slog.Default() if nil
}

// Run polls for claimable jobs until ctx is cancelled, invoking handler
// once per claimed job. Each claimed batch is committed (releasing the
// row locks) before handler is invoked, so a slow/failing handler never
// holds a database transaction open.
//
// handler is responsible for eventually moving the job to a terminal
// status ('done'/'failed') — that status transition is intentionally left
// to the caller/handler since it varies per job_type, and is not
// prescribed by this generic Poller.
func (p *Poller) Run(ctx context.Context, handler func(Job) error) error {
	batchSize := p.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}
	interval := p.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	logger := p.Logger
	if logger == nil {
		logger = slog.Default()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.pollOnce(ctx, batchSize, handler, logger); err != nil {
				logger.Error("pgqueue: poll failed", slog.Any("error", err))
			}
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, batchSize int, handler func(Job) error, logger *slog.Logger) error {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgqueue: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	jobs, err := ClaimJobs(ctx, tx, batchSize)
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgqueue: commit claim: %w", err)
	}

	for _, job := range jobs {
		go func(j Job) {
			if err := handler(j); err != nil {
				logger.Error("pgqueue: job handler failed",
					slog.Int64("job_id", j.ID),
					slog.String("job_type", j.JobType),
					slog.Any("error", err),
				)
			}
		}(job)
	}
	return nil
}
