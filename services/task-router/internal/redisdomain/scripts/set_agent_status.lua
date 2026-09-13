-- set_agent_status.lua
--
-- Implements "Set Agent Master Status" (spec Section 3.2): updates status
-- and statusChangedAt atomically, gated on the agent existing. Status
-- string validity against the registry is checked by the Go/pgconfig
-- layer BEFORE this script runs (registry lives in Postgres, spec
-- Section 2.5), since this script only touches Redis.
--
-- KEYS[1] = agent hash key (tr:{tenant}:agent:{agentId})
--
-- ARGV[1] = status
-- ARGV[2] = statusChangedAt (RFC3339Nano)
--
-- Returns: 1 on success, 0 if the agent does not exist.

if redis.call('EXISTS', KEYS[1]) ~= 1 then
  return 0
end

redis.call('HSET', KEYS[1], 'status', ARGV[1], 'statusChangedAt', ARGV[2])

return 1
