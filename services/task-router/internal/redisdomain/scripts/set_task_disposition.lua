-- set_task_disposition.lua
--
-- Implements "SetTaskDisposition" (step 2's data element, Wrap Up /
-- Disposition lifecycle): an agent is free to set a disposition any time
-- during WrapUp, up until the wrap-up timer reaches 0. Rejected unless the
-- task is currently WrapUp. disposition_id validity against the tenant's
-- Disposition registry (Postgres, pgconfig) is checked by the Go caller
-- BEFORE invoking this script -- this script only performs the atomic
-- WrapUp-status re-check and write, mirroring how reject_reservation.lua
-- assumes upstream validation for anything not itself a race condition.
--
-- KEYS[1] = task hash key (tr:{tenant}:task:{taskId})
--
-- ARGV[1] = taskId
-- ARGV[2] = dispositionId
-- ARGV[3] = dispositionName (denormalized copy, spec: Task.disposition_name)
--
-- Returns:
--   {1} on success
--   {0, currentStatus} if the task is not WrapUp (currentStatus = '' if
--     the task does not exist at all)

local dispositionId = ARGV[2]
local dispositionName = ARGV[3]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'WrapUp' then
  return {0, status}
end

redis.call('HSET', KEYS[1], 'dispositionId', dispositionId, 'dispositionName', dispositionName)

return {1}
