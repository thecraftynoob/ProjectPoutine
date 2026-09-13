-- delete_agent.lua
--
-- Implements spec Section 5.4 rule 5 and Section 5.5 (agent deletion while
-- holding work) as ONE atomic operation, not a check-then-act sequence.
-- Determining "does this agent have any Reserved/Active tasks, and if so
-- what reservations are tied to them" and acting on that happens inside
-- this single script, so no other operation can complete/resolve a task
-- or reservation in a gap between a read and a later write.
--
-- Behavior:
--   - The agent record and its set membership are removed unconditionally
--     (agent deletion always "succeeds" if the agent exists).
--   - An agent may legitimately hold MULTIPLE concurrent in-flight
--     (Offered or Accepted) reservations at once -- spec Section 2.1's
--     ChannelCapacity.max is a *concurrent* limit (confirmed by Section
--     3.2's capacity table and Section 4.2's accounting rules), so e.g.
--     max:3 on one channel lets the matching algorithm commit up to 3
--     simultaneous Reservations to the same agent, and this holds across
--     different channels on the same agent too. The agent's in-flight set
--     (KEYS[3]) tracks EVERY reservation currently Offered or Accepted
--     for this agent (see match_commit.lua / accept_reservation.lua /
--     complete_task.lua for the rest of that set's lifecycle), so this
--     script resolves EVERY member found there, not just one.
--   - For each one: the tied task (Reserved or Active) is atomically
--     reset to Pending at its ORIGINAL enqueuedAt (preserving FIFO order
--     per spec Section 5.1/5.5), and its tied Offered/Accepted
--     reservation is marked Rejected (reason: agent_deleted). Capacity
--     does not need separate release bookkeeping since the entire agent
--     record (and thus its capacity map) is deleted.
--   - Every accurate resolution is reported back to the Go caller (spec
--     Section 5.5's "exactly one accurate notification" guarantee applied
--     per-resolution: no false ones, and -- critically -- none silently
--     dropped when there is more than one) via the return value, so the
--     Go layer publishes exactly the right event(s), one per resolution.
--
-- KEYS[1] = agents set key                  (tr:{tenant}:agents)
-- KEYS[2] = agent hash key                  (tr:{tenant}:agent:{agentId})
-- KEYS[3] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers)
-- KEYS[4] = pending tasks ZSET key          (tr:{tenant}:tasks:pending)
--
-- ARGV[1] = agentId
-- ARGV[2] = tasksKeyPrefix        (e.g. "tr:{tenant}:task:")
-- ARGV[3] = reservationsKeyPrefix (e.g. "tr:{tenant}:reservation:")
-- ARGV[4] = resexpKeyPrefix       (e.g. "tr:{tenant}:resexp:")
--
-- Returns:
--   {0} if the agent did not exist (no-op).
--   {1, 0} if the agent existed and had no in-flight reservation to
--     resolve.
--   {1, N, reservationId_1, taskId_1, reservationId_2, taskId_2, ...} if
--     the agent existed and N (>=1) in-flight Offered/Accepted
--     reservations (and their tasks) were resolved as described above.

local agentId = ARGV[1]
local tasksPrefix = ARGV[2]
local reservationsPrefix = ARGV[3]
local resexpPrefix = ARGV[4]

if redis.call('SISMEMBER', KEYS[1], agentId) ~= 1 then
  return {0}
end

-- Every reservation currently Offered or Accepted for this agent (see
-- header comment) -- zero, one, or many.
local inFlight = redis.call('SMEMBERS', KEYS[3])

redis.call('SREM', KEYS[1], agentId)
redis.call('DEL', KEYS[2])
redis.call('DEL', KEYS[3])

local resolved = {}

for _, reservationId in ipairs(inFlight) do
  local reservationKey = reservationsPrefix .. reservationId
  local resStatus = redis.call('HGET', reservationKey, 'status')
  if resStatus == 'Offered' or resStatus == 'Accepted' then
    local taskId = redis.call('HGET', reservationKey, 'taskId')
    local taskKey = tasksPrefix .. taskId

    redis.call('HSET', reservationKey, 'status', 'Rejected', 'reason', 'agent_deleted')
    redis.call('DEL', resexpPrefix .. reservationId)

    local enqueuedMicros = redis.call('HGET', taskKey, 'enqueuedAtMicros')
    redis.call('HSET', taskKey,
      'status', 'Pending',
      'currentReservationId', '',
      'assignedAgentId', ''
    )
    if enqueuedMicros ~= false then
      redis.call('ZADD', KEYS[4], tonumber(enqueuedMicros), taskId)
    end

    table.insert(resolved, reservationId)
    table.insert(resolved, taskId)
  end
  -- else: already resolved by something else in a genuine race (e.g. a
  -- stale in-flight-set entry); nothing further to do for this one.
end

if #resolved == 0 then
  return {1, 0}
end

local result = {1, #resolved / 2}
for _, v in ipairs(resolved) do
  table.insert(result, v)
end
return result
