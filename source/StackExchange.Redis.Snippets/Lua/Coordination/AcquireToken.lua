--[[ 
KEYS[1] = lock name
ARGV[1] = clientId
ARGV[2] = lease time in seconds
-- Return fencing token or -1  
]]

local tokensouce = KEYS[1] .. '_token'
if (redis.call('EXISTS', KEYS[1]) == 1) then

	if (redis.call('GET', KEYS[1]) == ARGV[1]) then
		return redis.call('INCRBY', tokensouce, 1) -- new fencing token
	end

	return -1 -- cannot acquire lock
end

redis.call('SETEX', KEYS[1], ARGV[2], ARGV[1])

return redis.call('INCRBY', tokensouce, 1) -- new fencing token