-- enqueue_task.lua
--
-- Implements spec Section 5.4 rule 1 (uniqueness on creation) for Tasks:
-- atomically check-and-reserve the task ID. Also implements the FIFO
-- ordering primitive from Section 4.1 by inserting into the pending ZSET
-- scored by enqueuedAt at the same time the task hash is created, so a
-- concurrent matching pass can never observe a task hash without its
-- corresponding pending-set membership (or vice versa).
--
-- KEYS[1] = tasks set key             (tr:{tenant}:tasks)
-- KEYS[2] = task hash key             (tr:{tenant}:task:{id})
-- KEYS[3] = pending tasks ZSET key    (tr:{tenant}:tasks:pending)
--
-- ARGV[1] = taskId
-- ARGV[2] = queueId
-- ARGV[3] = taskType
-- ARGV[4] = requiredAttributes JSON
-- ARGV[5] = enqueuedAt (RFC3339Nano)
-- ARGV[6] = enqueuedAt unix micros (ZSET score)
-- ARGV[7] = wrapUpTimeoutSeconds (Wrap Up / Disposition lifecycle;
--           "0" means no wrap-up timer configured)
--
-- Returns: 1 on success, 0 if the task already exists.
--
-- enqueuedAtMicros is cached as its own hash field (not just the ZSET
-- score) so a later re-queue after a rejected reservation
-- (reject_reservation.lua) can restore the task's original
-- FIFO position purely from data already inside the task hash, without
-- any Go-side reparse/reformat of the timestamp string racing against
-- the atomic script boundary.

local taskId = ARGV[1]

if redis.call('SISMEMBER', KEYS[1], taskId) == 1 then
  return 0
end

redis.call('SADD', KEYS[1], taskId)
redis.call('HSET', KEYS[2],
  'queueId', ARGV[2],
  'taskType', ARGV[3],
  'requiredAttributes', ARGV[4],
  'enqueuedAt', ARGV[5],
  'enqueuedAtMicros', ARGV[6],
  'status', 'Pending',
  'currentReservationId', '',
  'assignedAgentId', '',
  'wrapUpTimeoutSeconds', ARGV[7],
  'dispositionId', '',
  'dispositionName', ''
)
redis.call('ZADD', KEYS[3], ARGV[6], taskId)

return 1
