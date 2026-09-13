-- complete_task.lua
--
-- Implements the "Complete a Task" capability (spec Section 3.3) and its
-- capacity-release rule (spec Section 4.2: decremented by exactly 1,
-- floor 0, when a task completes) as a single atomic operation (spec
-- Section 5.4 rule 2: capacity mutation is never read-modify-write from
-- application code).
--
-- Rejected unless the task is currently Active. Also removes JUST this
-- task's own tied reservation (read from the task's own
-- currentReservationId field) from the agent's in-flight reservations set
-- (see accept_reservation.lua / delete_agent.lua) -- NOT a blanket clear,
-- since an agent may hold several other concurrent Offered/Accepted
-- reservations at once (ChannelCapacity.max > 1, spec Sections 2.1, 3.2,
-- 4.2) whose own bookkeeping must be left untouched by this task's
-- completion.
--
-- KEYS[1] = task hash key   (tr:{tenant}:task:{taskId})
-- KEYS[2] = agent hash key  (tr:{tenant}:agent:{agentId}, agentId read
--           from the task's own assignedAgentId field)
-- KEYS[3] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers,
--           agentId read from the task's own assignedAgentId field)
--
-- ARGV[1] = taskId
--
-- Returns:
--   {1, agentId} on success (agentId may be '' if the assigned agent was
--     already deleted by the time of completion -- capacity release is
--     then a no-op since the agent record no longer exists)
--   {0, currentStatus} if the task is not Active (currentStatus = '' if
--     the task does not exist at all)

local taskId = ARGV[1]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'Active' then
  return {0, status}
end

local agentId = redis.call('HGET', KEYS[1], 'assignedAgentId')
local taskType = redis.call('HGET', KEYS[1], 'taskType')
local reservationId = redis.call('HGET', KEYS[1], 'currentReservationId')

redis.call('HSET', KEYS[1], 'status', 'Completed', 'currentReservationId', '')

if agentId ~= false and agentId ~= '' and redis.call('EXISTS', KEYS[2]) == 1 then
  local capField = 'cap:' .. taskType
  local capJSON = redis.call('HGET', KEYS[2], capField)
  if capJSON ~= false then
    local cap = cjson.decode(capJSON)
    cap.active = math.max(0, cap.active - 1)
    redis.call('HSET', KEYS[2], capField, cjson.encode(cap))
  end
  if reservationId ~= false and reservationId ~= '' then
    redis.call('SREM', KEYS[3], reservationId)
  end
end

return {1, agentId}
