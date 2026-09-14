package wrapupsync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thecraftynoob/ProjectPoutine/pkg/pgqueue"
	"github.com/thecraftynoob/ProjectPoutine/services/background-worker-pool/internal/pgstore"
)

// httpTimeout bounds every outbound wrap-up sync POST -- a few seconds,
// per this milestone's scope (no real CRM exists, so there is no
// production SLA to tune this against yet).
const httpTimeout = 5 * time.Second

// maxAttempts is the retry cutoff: a job is retried (with backoff, see
// nextRunAfter below) while its Attempts count (already incremented by
// ClaimJobs before the handler ever runs) is at or under this value, and
// marked permanently 'failed' once it exceeds it. This is a genuine scope
// decision, not a pkg/pgqueue default -- documented in GAPS.md ("jobs
// retry forever" vs. "jobs give up after N attempts").
const maxAttempts = 5

// baseBackoff and maxBackoff define this milestone's backoff formula:
// run_after = now + attempts*baseBackoff, capped at maxBackoff. A fixed
// linear-with-cap formula, not exponential-with-jitter -- deliberately
// simple, and documented as such in GAPS.md rather than left as an
// unstated simplification.
const (
	baseBackoff = 30 * time.Second
	maxBackoff  = 5 * time.Minute
)

// Handler dispatches claimed background_jobs by job_type. Only JobType
// ("wrapup_sync") is implemented this milestone; any other job_type is
// logged and marked 'failed' rather than crashing the poller or being
// silently dropped -- background_jobs is architected to carry multiple
// job types eventually (webhook_delivery, billing_rollup per
// pkg/pgqueue's own doc comment), so this dispatch structure is written
// to make adding a second job_type later a matter of adding a case, not
// restructuring.
type Handler struct {
	pool    *pgxpool.Pool
	targets *pgstore.WrapupTargetStore
	logger  *slog.Logger
	httpDo  func(*http.Request) (*http.Response, error)
}

// NewHandler constructs a Handler using a real net/http.Client with
// httpTimeout.
func NewHandler(pool *pgxpool.Pool, targets *pgstore.WrapupTargetStore, logger *slog.Logger) *Handler {
	client := &http.Client{Timeout: httpTimeout}
	return &Handler{pool: pool, targets: targets, logger: logger, httpDo: client.Do}
}

// HandleJob is the pgqueue.Poller handler entry point: dispatches by
// job_type, marking any unrecognized type 'failed' rather than leaving it
// stuck 'processing' forever or panicking the poller goroutine.
func (h *Handler) HandleJob(job pgqueue.Job) error {
	switch job.JobType {
	case JobType:
		return h.handleWrapupSync(context.Background(), job)
	default:
		h.logger.Error("unrecognized job_type, marking failed", slog.Int64("job_id", job.ID), slog.String("job_type", job.JobType))
		if err := pgqueue.MarkFailed(context.Background(), h.pool, job.ID, false, time.Time{}); err != nil {
			return fmt.Errorf("wrapupsync: mark unrecognized job_type job %d failed: %w", job.ID, err)
		}
		return nil
	}
}

// handleWrapupSync unmarshals the job's payload, looks up the tenant's
// configured wrap-up target URL, POSTs the payload as JSON, and marks the
// job done/failed(-or-retried) accordingly.
func (h *Handler) handleWrapupSync(ctx context.Context, job pgqueue.Job) error {
	logger := h.logger.With(slog.Int64("job_id", job.ID), slog.String("tenant_id", job.TenantID.String()))

	var payload WrapupSyncPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		// A malformed payload can never succeed on retry -- fail
		// permanently rather than retrying something that will never
		// parse differently.
		logger.Error("failed to unmarshal wrapup_sync payload, marking failed permanently", slog.Any("error", err))
		if markErr := pgqueue.MarkFailed(ctx, h.pool, job.ID, false, time.Time{}); markErr != nil {
			return fmt.Errorf("wrapupsync: mark unparseable job %d failed: %w", job.ID, markErr)
		}
		return nil
	}

	targetURL, err := h.targets.GetWrapupTargetURL(ctx, job.TenantID)
	if errors.Is(err, pgstore.ErrWrapupTargetNotFound) {
		// No target configured is a tenant-configuration gap, not a
		// transient failure -- retrying won't help until an operator
		// inserts a row (see GAPS.md's wrap-up-target-table entry), so
		// this is marked failed permanently rather than retried.
		logger.Error("no wrapup target URL configured for tenant, marking failed", slog.String("task_id", payload.TaskID))
		if markErr := pgqueue.MarkFailed(ctx, h.pool, job.ID, false, time.Time{}); markErr != nil {
			return fmt.Errorf("wrapupsync: mark job %d failed (no target): %w", job.ID, markErr)
		}
		return nil
	}
	if err != nil {
		logger.Error("failed to look up wrapup target url", slog.Any("error", err))
		return h.retryOrFail(ctx, job, logger, fmt.Errorf("look up wrapup target url: %w", err))
	}

	body, err := json.Marshal(payload)
	if err != nil {
		logger.Error("failed to marshal outbound wrapup payload, marking failed permanently", slog.Any("error", err))
		if markErr := pgqueue.MarkFailed(ctx, h.pool, job.ID, false, time.Time{}); markErr != nil {
			return fmt.Errorf("wrapupsync: mark job %d failed (marshal): %w", job.ID, markErr)
		}
		return nil
	}

	reqCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		logger.Error("failed to build outbound request, marking failed permanently", slog.String("target_url", targetURL), slog.Any("error", err))
		if markErr := pgqueue.MarkFailed(ctx, h.pool, job.ID, false, time.Time{}); markErr != nil {
			return fmt.Errorf("wrapupsync: mark job %d failed (build request): %w", job.ID, markErr)
		}
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpDo(req)
	if err != nil {
		logger.Warn("wrapup sync POST failed", slog.String("target_url", targetURL), slog.Any("error", err))
		return h.retryOrFail(ctx, job, logger, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("wrapup sync POST returned non-2xx", slog.String("target_url", targetURL), slog.Int("status_code", resp.StatusCode))
		return h.retryOrFail(ctx, job, logger, fmt.Errorf("non-2xx response: %d", resp.StatusCode))
	}

	if err := pgqueue.MarkDone(ctx, h.pool, job.ID); err != nil {
		return fmt.Errorf("wrapupsync: mark job %d done: %w", job.ID, err)
	}
	logger.Info("wrapup sync POST succeeded", slog.String("task_id", payload.TaskID))
	return nil
}

// retryOrFail marks a job 'pending' again with a backed-off run_after if
// it hasn't yet exceeded maxAttempts, otherwise marks it permanently
// 'failed'. job.Attempts already reflects THIS attempt (ClaimJobs
// increments it as part of the claiming UPDATE, before the handler ever
// runs).
func (h *Handler) retryOrFail(ctx context.Context, job pgqueue.Job, logger *slog.Logger, cause error) error {
	if job.Attempts < maxAttempts {
		retryAt := nextRunAfter(job.Attempts)
		logger.Info("scheduling retry", slog.Int("attempts", job.Attempts), slog.Time("run_after", retryAt), slog.Any("cause", cause))
		if err := pgqueue.MarkFailed(ctx, h.pool, job.ID, true, retryAt); err != nil {
			return fmt.Errorf("wrapupsync: schedule retry for job %d: %w", job.ID, err)
		}
		return nil
	}
	logger.Error("exceeded max attempts, marking failed permanently", slog.Int("attempts", job.Attempts), slog.Any("cause", cause))
	if err := pgqueue.MarkFailed(ctx, h.pool, job.ID, false, time.Time{}); err != nil {
		return fmt.Errorf("wrapupsync: mark exhausted job %d failed: %w", job.ID, err)
	}
	return nil
}

// nextRunAfter implements this milestone's backoff formula: a fixed
// linear delay (attempts * baseBackoff) capped at maxBackoff -- not
// exponential-with-jitter. See GAPS.md for why this simplicity is a
// documented scope decision, not an oversight.
func nextRunAfter(attempts int) time.Time {
	delay := time.Duration(attempts) * baseBackoff
	if delay > maxBackoff {
		delay = maxBackoff
	}
	return time.Now().Add(delay)
}
