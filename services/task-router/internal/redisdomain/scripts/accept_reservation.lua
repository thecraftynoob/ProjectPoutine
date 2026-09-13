-- accept_reservation.lua
--
-- Implements spec Section 5.4 rule 4: a reservation's terminal resolution
-- must atomically re-check that the reservation is still "Offered" before
-- applying any change, and must return a distinguishable "already
-- resolved" result rather than silently double-applying.
--
-- On success: reservation -> Accepted, task -> Active. Capacity is
-- untouched (accept does not change active count; it was already
-- incremented at match-commit time). The reservation is deliberately
-- LEFT in the agent's in-flight set (KEYS[3]) rather than removed --
-- that set tracks every reservation currently Offered OR Accepted for
-- this agent, not just Offered ones, so delete_agent.lua can find and
-- resolve EVERY in-flight reservation an agent holds, not just one (an
-- agent may legitimately hold several concurrent Offered/Accepted
-- reservations at once under ChannelCapacity.max > 1 -- spec Sections
-- 2.1, 3.2, 4.2). It is only removed from this set on a terminal
-- resolution: here that means complete_task.lua (Accepted -> Completed)
-- or delete_agent.lua (agent-deletion-triggered Rejected).
--
-- KEYS[1] = reservation hash key   (tr:{tenant}:reservation:{id})
-- KEYS[2] = task hash key          (tr:{tenant}:task:{taskId})
-- KEYS[3] = agent in-flight reservations set key (tr:{tenant}:agent:{agentId}:offers)
-- KEYS[4] = reservation expiry sentinel key (tr:{tenant}:resexp:{id})
--
-- ARGV[1] = reservationId
--
-- Returns:
--   {1, taskId, agentId} on success
--   {0, currentStatus} if not Offered (already resolved by something else,
--     or reservation does not exist -> currentStatus = "")

local reservationId = ARGV[1]

local status = redis.call('HGET', KEYS[1], 'status')
if status == false then
  return {0, ''}
end
if status ~= 'Offered' then
  return {0, status}
end

local taskId = redis.call('HGET', KEYS[1], 'taskId')
local agentId = redis.call('HGET', KEYS[1], 'agentId')

redis.call('HSET', KEYS[1], 'status', 'Accepted')
redis.call('HSET', KEYS[2], 'status', 'Active')
-- Deliberately NOT removed from KEYS[3] -- see header comment: it stays
-- tracked as in-flight (now Accepted) until a terminal resolution.
redis.call('DEL', KEYS[4])

return {1, taskId, agentId}
