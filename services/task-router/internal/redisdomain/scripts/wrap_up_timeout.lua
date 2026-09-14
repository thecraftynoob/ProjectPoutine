-- wrap_up_timeout.lua
--
-- Resolves a fired wrap-up-timer sentinel key (see keys.go's
-- wrapUpExpiryKey, driven by Redis keyspace notifications exactly like
-- reservation expiry -- see expiry.go/SubscribeExpiry and this script's
-- Go-side caller). Per the Wrap Up / Disposition lifecycle spec: "If the
-- Agent's Wrap up timer reaches 0 (meaning the full time was used) the
-- agent's status needs to be updated to Available." The task itself is
-- NOT touched -- it stays WrapUp until an explicit CompleteTask (this
-- mirrors reject_reservation.lua's atomic re-validation pattern: only
-- apply the side effect if the task is STILL in WrapUp, since the task may
-- have already been completed between the timer firing and this handler
-- running, which the Go layer's proactive KEYS[1]... DEL in
-- complete_task.lua normally prevents, but a redelivered/racing
-- notification is still possible -- see spec Section 5.4 rule 4's general
-- idempotent-safe-resolution principle, applied here).
--
-- KEYS[1] = task hash key  (tr:{tenant}:task:{taskId})
-- KEYS[2] = agent hash key (tr:{tenant}:agent:{agentId}, agentId read
--           from the task's own assignedAgentId field)
--
-- ARGV[1] = statusChangedAt (RFC3339Nano), for the agent's Available
--           transition
--
-- Returns:
--   {1, agentId} if the agent's status was reset to Available
--   {0, currentStatus} if the task is no longer WrapUp (no-op;
--     currentStatus = '' if the task does not exist at all)

local statusChangedAt = ARGV[1]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'WrapUp' then
  return {0, status}
end

local agentId = redis.call('HGET', KEYS[1], 'assignedAgentId')

if agentId ~= false and agentId ~= '' and redis.call('EXISTS', KEYS[2]) == 1 then
  redis.call('HSET', KEYS[2], 'status', 'Available', 'statusChangedAt', statusChangedAt)
  return {1, agentId}
end

return {1, ''}
