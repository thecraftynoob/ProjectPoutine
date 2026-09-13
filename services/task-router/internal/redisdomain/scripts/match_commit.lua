-- match_commit.lua
--
-- Implements spec Section 5.4 rule 3: the match-commit step must
-- re-validate all matching conditions atomically at commit time, not
-- merely trust an earlier scan. This is what makes the matching algorithm
-- (spec Section 4.1) safe under concurrent mutation, and per rule 6, safe
-- across multiple concurrent Task Router replicas with no external
-- locking — two replicas racing to match the same agent/task pair will
-- have exactly one script invocation observe pre-commit state and the
-- other observe post-commit state, so only one can succeed.
--
-- Re-checks, in order, exactly the agent_can_take gates from spec Section
-- 4.1 (no ready/interruptible/attribute checks), PLUS that the task is
-- still Pending (it may have been claimed by a different match in the
-- gap between scan and commit):
--   1. task.status == "Pending"
--   2. agent exists and agent.status == "Available"
--   3. task.queueId is in agent.queues
--   4. agent.capacity[task.taskType] exists
--   5. agent.capacity[task.taskType].active < .max
--
-- On success: increments the channel's active count by exactly 1 (spec
-- Section 4.2), removes the task from the pending ZSET, sets task status
-- to "Reserved" with currentReservationId/assignedAgentId, creates the
-- Reservation hash (status "Offered"), adds it to the agent's offers set,
-- and sets the TTL sentinel key for expiry.
--
-- KEYS[1] = agent hash key                 (tr:{tenant}:agent:{agentId})
-- KEYS[2] = task hash key                  (tr:{tenant}:task:{taskId})
-- KEYS[3] = pending tasks ZSET key         (tr:{tenant}:tasks:pending)
-- KEYS[4] = reservation hash key           (tr:{tenant}:reservation:{reservationId})
-- KEYS[5] = reservations set key           (tr:{tenant}:reservations)
-- KEYS[6] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers)
--           Tracks every reservation currently Offered OR Accepted for
--           this agent (an agent may legitimately hold several
--           concurrent in-flight reservations at once under
--           ChannelCapacity.max > 1 -- spec Sections 2.1, 3.2, 4.2), only
--           removed on terminal resolution (Accepted->Completed, or
--           Offered/Accepted->Rejected). See accept_reservation.lua and
--           delete_agent.lua for the rest of this set's lifecycle.
-- KEYS[7] = reservation expiry sentinel key (tr:{tenant}:resexp:{reservationId})
--
-- ARGV[1] = taskId
-- ARGV[2] = agentId
-- ARGV[3] = reservationId
-- ARGV[4] = createdAt (RFC3339Nano)
-- ARGV[5] = expiresAt (RFC3339Nano)
-- ARGV[6] = ttlSeconds (integer, for the sentinel key's TTL)
--
-- Returns:
--   {1, expiresAt} on success
--   {0, reason} on refusal, reason in:
--     "task_not_pending" | "agent_missing" | "agent_not_available" |
--     "queue_mismatch" | "channel_missing" | "channel_full"

local agentKey = KEYS[1]
local taskKey = KEYS[2]
local pendingKey = KEYS[3]
local reservationKey = KEYS[4]
local reservationsSetKey = KEYS[5]
local agentOffersKey = KEYS[6]
local expiryKey = KEYS[7]

local taskId = ARGV[1]
local agentId = ARGV[2]
local reservationId = ARGV[3]
local createdAt = ARGV[4]
local expiresAt = ARGV[5]
local ttlSeconds = tonumber(ARGV[6])

-- Gate: task must still be Pending.
local taskStatus = redis.call('HGET', taskKey, 'status')
if taskStatus ~= 'Pending' then
  return {0, 'task_not_pending'}
end

-- Gate 1: agent must exist and be Available.
local agentStatus = redis.call('HGET', agentKey, 'status')
if agentStatus == false then
  return {0, 'agent_missing'}
end
if agentStatus ~= 'Available' then
  return {0, 'agent_not_available'}
end

-- Gate 2a: queue membership.
local queueId = redis.call('HGET', taskKey, 'queueId')
local queuesJSON = redis.call('HGET', agentKey, 'queues')
if queuesJSON == false then
  queuesJSON = '[]'
end
local queues = cjson.decode(queuesJSON)
local isMember = false
for _, q in ipairs(queues) do
  if q == queueId then
    isMember = true
    break
  end
end
if not isMember then
  return {0, 'queue_mismatch'}
end

-- Gate 2b/2c: channel exists and has headroom.
local taskType = redis.call('HGET', taskKey, 'taskType')
local capField = 'cap:' .. taskType
local capJSON = redis.call('HGET', agentKey, capField)
if capJSON == false then
  return {0, 'channel_missing'}
end
local cap = cjson.decode(capJSON)
if cap.active >= cap.max then
  return {0, 'channel_full'}
end

-- All gates passed: commit atomically.
cap.active = cap.active + 1
redis.call('HSET', agentKey, capField, cjson.encode(cap))

redis.call('ZREM', pendingKey, taskId)
redis.call('HSET', taskKey,
  'status', 'Reserved',
  'currentReservationId', reservationId,
  'assignedAgentId', agentId
)

redis.call('HSET', reservationKey,
  'taskId', taskId,
  'agentId', agentId,
  'status', 'Offered',
  'createdAt', createdAt,
  'expiresAt', expiresAt,
  'reason', ''
)
redis.call('SADD', reservationsSetKey, reservationId)
redis.call('SADD', agentOffersKey, reservationId) -- in-flight: Offered

redis.call('SET', expiryKey, '1', 'EX', ttlSeconds)

return {1, expiresAt}
