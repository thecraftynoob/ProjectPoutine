-- end_task.lua
--
-- Implements "EndTask" (step 1 of the two-step Wrap Up / Disposition
-- completion lifecycle): stopping the communication channel for an Active
-- task moves it to WrapUp and the assigned agent's status to "WrapUp",
-- WITHOUT releasing capacity or formally completing the interaction --
-- that only happens via CompleteTask (see complete_task.lua). Rejected
-- unless the task is currently Active.
--
-- KEYS[1] = task hash key   (tr:{tenant}:task:{taskId})
-- KEYS[2] = agent hash key  (tr:{tenant}:agent:{agentId}, agentId read
--           from the task's own assignedAgentId field)
--
-- ARGV[1] = taskId
-- ARGV[2] = statusChangedAt (RFC3339Nano), for the agent's WrapUp
--           transition
--
-- Returns:
--   {1, agentId} on success (agentId may be '' if the assigned agent was
--     already deleted -- the task still moves to WrapUp regardless)
--   {0, currentStatus} if the task is not Active (currentStatus = '' if
--     the task does not exist at all)

local taskId = ARGV[1]
local statusChangedAt = ARGV[2]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'Active' then
  return {0, status}
end

local agentId = redis.call('HGET', KEYS[1], 'assignedAgentId')

redis.call('HSET', KEYS[1], 'status', 'WrapUp')

if agentId ~= false and agentId ~= '' and redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('HSET', KEYS[2], 'status', 'WrapUp', 'statusChangedAt', statusChangedAt)
end

return {1, agentId}
