-- replace_capacity.lua
--
-- Implements "Replace Agent Capacity Map" (spec Section 3.2) and its
-- accounting rules (spec Section 4.2) as a single atomic operation (spec
-- Section 5.4 rule 2): any channel omitted from the input is removed;
-- `active` supplied by the caller is always ignored -- a retained channel
-- keeps its live `active` count carried forward, a new channel starts at
-- 0. Atomic against a concurrent match (a concurrent match_commit.lua
-- invocation for this agent either fully happens before or fully after
-- this script, never interleaved).
--
-- KEYS[1] = agent hash key (tr:{tenant}:agent:{agentId})
--
-- ARGV[1] = agentId (unused directly, kept for symmetry/logging)
-- ARGV[2] = channelCount (N)
-- ARGV[3..2+2N] = pairs of (channelName, capacityJSON-without-active)
--   where capacityJSON is {"ready":bool,"max":int,"interruptible":bool}
--   (no "active" key -- this script computes it)
--
-- Returns: 1 on success, 0 if the agent does not exist.

local agentKey = KEYS[1]

if redis.call('EXISTS', agentKey) ~= 1 then
  return 0
end

-- Discover existing cap:* fields so we can remove any channel not present
-- in the new input.
local existingFields = redis.call('HKEYS', agentKey)
local existingChannels = {}
for _, f in ipairs(existingFields) do
  local channel = string.match(f, '^cap:(.+)$')
  if channel then
    existingChannels[channel] = f
  end
end

local newChannels = {}
local channelCount = tonumber(ARGV[2])
local idx = 3
for i = 1, channelCount do
  local channel = ARGV[idx]
  local capJSON = ARGV[idx + 1]
  idx = idx + 2

  local cap = cjson.decode(capJSON)
  local oldField = existingChannels[channel]
  if oldField ~= nil then
    local oldJSON = redis.call('HGET', agentKey, oldField)
    local oldCap = cjson.decode(oldJSON)
    cap.active = oldCap.active
  else
    cap.active = 0
  end
  redis.call('HSET', agentKey, 'cap:' .. channel, cjson.encode(cap))
  newChannels[channel] = true
end

-- Remove channels that existed before but are absent from the new input.
for channel, field in pairs(existingChannels) do
  if not newChannels[channel] then
    redis.call('HDEL', agentKey, field)
  end
end

return 1
