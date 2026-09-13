-- replace_agent_attributes.lua
--
-- Implements "Replace Agent Attributes" (spec Section 3.2): full replace,
-- gated on the agent existing. Shape validation against the Attribute
-- registry (spec Section 4.4) happens in the Go/pgconfig layer BEFORE
-- this script runs.
--
-- KEYS[1] = agent hash key (tr:{tenant}:agent:{agentId})
--
-- ARGV[1] = attributes JSON
--
-- Returns: 1 on success, 0 if the agent does not exist.

if redis.call('EXISTS', KEYS[1]) ~= 1 then
  return 0
end

redis.call('HSET', KEYS[1], 'attributes', ARGV[1])

return 1
