-- complete_task.lua
--
-- Implements the "Complete a Task" capability (spec Section 3.3) and its
-- capacity-release rule (spec Section 4.2: decremented by exactly 1,
-- floor 0, when a task completes) as a single atomic operation (spec
-- Section 5.4 rule 2: capacity mutation is never read-modify-write from
-- application code).
--
-- Extended for the Wrap Up / Disposition two-step completion lifecycle:
-- valid from EITHER Active (a task with no wrap_up_timeout_seconds
-- configured can be completed directly, skipping WrapUp entirely -- the
-- original, unmodified behavior) OR WrapUp (the agent clicked Complete
-- Task during wrap-up, optionally after SetTaskDisposition). Either way,
-- CompleteTask formally completes the interaction and resets the assigned
-- agent's status to "Available" -- a new side effect that did not exist
-- before this lifecycle was introduced (previously this script never
-- touched agent status at all, since capacity/eligibility alone governed
-- matching).
--
-- Also removes JUST this task's own tied reservation (read from the
-- task's own currentReservationId field) from the agent's in-flight
-- reservations set (see accept_reservation.lua / delete_agent.lua) -- NOT
-- a blanket clear, since an agent may hold several other concurrent
-- Offered/Accepted reservations at once (ChannelCapacity.max > 1, spec
-- Sections 2.1, 3.2, 4.2) whose own bookkeeping must be left untouched by
-- this task's completion. Also deletes the task's wrap-up expiry sentinel
-- key (a no-op DEL if the task was completed straight from Active and
-- never had one), so a stale wrap-up-timer notification can never fire
-- for an already-completed task.
--
-- KEYS[1] = task hash key   (tr:{tenant}:task:{taskId})
-- KEYS[2] = agent hash key  (tr:{tenant}:agent:{agentId}, agentId read
--           from the task's own assignedAgentId field)
-- KEYS[3] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers,
--           agentId read from the task's own assignedAgentId field)
-- KEYS[4] = wrap-up expiry sentinel key (tr:{tenant}:wrapupexp:{taskId})
--
-- ARGV[1] = taskId
-- ARGV[2] = statusChangedAt (RFC3339Nano), for the agent's Available
--           transition
--
-- Returns:
--   {1, agentId} on success (agentId may be '' if the assigned agent was
--     already deleted by the time of completion -- capacity release is
--     then a no-op since the agent record no longer exists)
--   {0, currentStatus} if the task is neither Active nor WrapUp
--     (currentStatus = '' if the task does not exist at all)

local taskId = ARGV[1]
local statusChangedAt = ARGV[2]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'Active' and status ~= 'WrapUp' then
  return {0, status}
end

local agentId = redis.call('HGET', KEYS[1], 'assignedAgentId')
local taskType = redis.call('HGET', KEYS[1], 'taskType')
local reservationId = redis.call('HGET', KEYS[1], 'currentReservationId')

redis.call('HSET', KEYS[1], 'status', 'Completed', 'currentReservationId', '')
redis.call('DEL', KEYS[4])

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
  redis.call('HSET', KEYS[2], 'status', 'Available', 'statusChangedAt', statusChangedAt)
end

return {1, agentId}
