-- toggle_channel_ready.lua
--
-- Implements "Toggle One Channel's Ready Flag" (spec Section 3.2) as a
-- single atomic HGET+HSET (spec Section 5.4 rule 2 -- this still counts
-- as "never read-modify-write from ordinary application code" because
-- the read and write are both inside this atomic script, not round-
-- tripped through Go). If the channel does not exist yet, it is created
-- with default max=1, interruptible=true (spec Section 3.2).
--
-- KEYS[1] = agent hash key (tr:{tenant}:agent:{agentId})
--
-- ARGV[1] = channel
-- ARGV[2] = ready ("1" or "0")
--
-- Returns: 1 on success, 0 if the agent does not exist.

local agentKey = KEYS[1]
local channel = ARGV[1]
local ready = ARGV[2] == '1'

if redis.call('EXISTS', agentKey) ~= 1 then
  return 0
end

local field = 'cap:' .. channel
local existingJSON = redis.call('HGET', agentKey, field)

local cap
if existingJSON == false then
  cap = { ready = ready, max = 1, active = 0, interruptible = true }
else
  cap = cjson.decode(existingJSON)
  cap.ready = ready
end

redis.call('HSET', agentKey, field, cjson.encode(cap))

return 1
