-- reject_reservation.lua
--
-- Implements spec Section 5.4 rule 4 (idempotent-safe terminal
-- resolution) for the Rejected outcome, used by BOTH the manual-reject
-- capability (spec Section 3.4) and the automatic expiry sweep (spec
-- Section 4.3) -- the two are functionally identical mutations,
-- distinguished only by the "reason" argument (spec Sections 5.2, 6.2).
-- (Agent deletion's own in-flight-task cleanup is a separate script,
-- delete_agent.lua, because it must also atomically handle agent removal
-- itself -- see spec Section 5.4 rule 5 and Section 5.5.)
--
-- Atomically re-checks the reservation is still "Offered" before applying
-- any change (never overwrites an already-Accepted or already-Rejected
-- reservation), releases the consumed capacity (floor 0), returns the
-- task to Pending at its ORIGINAL enqueuedAt (preserving FIFO order), and
-- sets the agent's status to "Not Responding" (spec Section 5.3) -- this
-- script assumes the agent still exists; the caller (Go layer) is
-- expected to only invoke this for the manual-reject/expiry paths, where
-- the agent is a live actor. Agent-deletion's own path never calls this
-- script (see delete_agent.lua) so no agent-existence branch is needed
-- here.
--
-- KEYS[1] = reservation hash key       (tr:{tenant}:reservation:{id})
-- KEYS[2] = task hash key              (tr:{tenant}:task:{taskId})
-- KEYS[3] = pending tasks ZSET key     (tr:{tenant}:tasks:pending)
-- KEYS[4] = agent hash key             (tr:{tenant}:agent:{agentId})
-- KEYS[5] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers)
--           Only THIS reservation is removed -- an agent may have other
--           concurrent in-flight reservations (ChannelCapacity.max > 1,
--           spec Sections 2.1, 3.2, 4.2) whose bookkeeping must survive.
-- KEYS[6] = reservation expiry sentinel key (tr:{tenant}:resexp:{id})
--
-- ARGV[1] = reservationId
-- ARGV[2] = reason ("agent_rejected" | "expired")
-- ARGV[3] = statusChangedAt (RFC3339Nano), for the agent's
--           Not-Responding transition
--
-- Returns:
--   {1, taskId, agentId} on success
--   {0, currentStatus} if not Offered (already resolved; "" if the
--     reservation does not exist at all)

local reservationId = ARGV[1]
local reason = ARGV[2]
local statusChangedAt = ARGV[3]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'Offered' then
  return {0, status}
end

local taskId = redis.call('HGET', KEYS[1], 'taskId')
local agentId = redis.call('HGET', KEYS[1], 'agentId')

redis.call('HSET', KEYS[1], 'status', 'Rejected', 'reason', reason)

-- Release capacity (floor 0).
local taskType = redis.call('HGET', KEYS[2], 'taskType')
local capField = 'cap:' .. taskType
local capJSON = redis.call('HGET', KEYS[4], capField)
if capJSON ~= false then
  local cap = cjson.decode(capJSON)
  cap.active = math.max(0, cap.active - 1)
  redis.call('HSET', KEYS[4], capField, cjson.encode(cap))
end

-- Return task to Pending at its ORIGINAL enqueuedAt.
local enqueuedAt = redis.call('HGET', KEYS[2], 'enqueuedAt')
redis.call('HSET', KEYS[2],
  'status', 'Pending',
  'currentReservationId', '',
  'assignedAgentId', ''
)
local enqueuedMicros = redis.call('HGET', KEYS[2], 'enqueuedAtMicros')
if enqueuedMicros == false then
  -- Fallback: score not separately cached, re-add with 0 (should not
  -- happen in practice since enqueue_task.lua always sets the ZSET
  -- entry with a numeric score at creation and this script never removes
  -- the cached score field -- defensive only).
  redis.call('ZADD', KEYS[3], 0, taskId)
else
  redis.call('ZADD', KEYS[3], tonumber(enqueuedMicros), taskId)
end

redis.call('SREM', KEYS[5], reservationId)
redis.call('DEL', KEYS[6])

-- Agent side effect: Not Responding (spec Section 5.3).
if redis.call('EXISTS', KEYS[4]) == 1 then
  redis.call('HSET', KEYS[4], 'status', 'Not Responding', 'statusChangedAt', statusChangedAt)
end

return {1, taskId, agentId}
