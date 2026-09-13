-- create_agent.lua
--
-- Implements spec Section 5.4 rule 1 (uniqueness on creation): atomically
-- check-and-reserve the agent ID so two simultaneous CreateAgent calls for
-- the same ID can never both succeed. One wins; the other gets a clear
-- "already exists" rejection.
--
-- KEYS[1] = agents set key            (tr:{tenant}:agents)
-- KEYS[2] = agent hash key            (tr:{tenant}:agent:{id})
--
-- ARGV[1] = agentId
-- ARGV[2] = status
-- ARGV[3] = statusChangedAt (RFC3339Nano)
-- ARGV[4] = attributes JSON
-- ARGV[5] = queues JSON
-- ARGV[6] = capacityCount (N) -- number of channel entries following
-- ARGV[7..6+2N] = pairs of (channelName, capacityJSON)
--
-- Returns: 1 on success, 0 if the agent already exists.

local agentId = ARGV[1]

if redis.call('SISMEMBER', KEYS[1], agentId) == 1 then
  return 0
end

redis.call('SADD', KEYS[1], agentId)
redis.call('HSET', KEYS[2],
  'status', ARGV[2],
  'statusChangedAt', ARGV[3],
  'attributes', ARGV[4],
  'queues', ARGV[5]
)

local capCount = tonumber(ARGV[6])
local idx = 7
for i = 1, capCount do
  local channel = ARGV[idx]
  local capJSON = ARGV[idx + 1]
  redis.call('HSET', KEYS[2], 'cap:' .. channel, capJSON)
  idx = idx + 2
end

return 1
