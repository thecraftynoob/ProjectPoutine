-- replace_agent_queues.lua
--
-- Implements "Replace Agent Queue Memberships" (spec Section 3.2): full
-- replace, gated on the agent existing. Queue-ID existence validation
-- happens in the Go/pgconfig layer BEFORE this script runs (the Queue
-- registry lives in Postgres, spec Section 2.4) -- "rejected entirely, no
-- partial application, if any queue ID doesn't exist" is naturally
-- satisfied by validating before ever calling this script.
--
-- KEYS[1] = agent hash key (tr:{tenant}:agent:{agentId})
--
-- ARGV[1] = queues JSON array
--
-- Returns: 1 on success, 0 if the agent does not exist.

if redis.call('EXISTS', KEYS[1]) ~= 1 then
  return 0
end

redis.call('HSET', KEYS[1], 'queues', ARGV[1])

return 1
