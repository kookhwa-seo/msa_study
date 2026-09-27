-- 토큰 버킷 rate limiter. Redis에서 원자적으로 실행돼야 하는 이유: "지금 토큰이 몇 개
-- 남았는지 읽고 -> 계산하고 -> 줄여서 쓴다"를 애플리케이션 쪽에서 나눠서 하면, 동시 요청
-- 여러 개가 같은 버킷을 동시에 읽어서 전부 "토큰 있음"으로 착각하는 경합이 생긴다
-- (docs/concurrent-booking-lab.md에서 다룬 check-then-act 레이스와 같은 종류의 문제).
-- Lua 스크립트는 Redis 안에서 한 번에 통째로 실행되기 때문에 이 경합이 원천적으로 없다.
--
-- KEYS[1] = 버킷 키 (rate-limit:<clientIp>)
-- ARGV[1] = capacity (버킷 최대 용량)
-- ARGV[2] = refill_per_second (초당 채워지는 토큰 수)
-- ARGV[3] = now_millis
-- ARGV[4] = requested (이번 요청이 쓰는 토큰 수, 보통 1)
--
-- 반환: {allowed(1/0), remaining_tokens}

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local capacity = tonumber(ARGV[1])
local refillPerSecond = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil then
    tokens = capacity
    ts = now
end

local elapsedMs = math.max(0, now - ts)
local refilled = elapsedMs * refillPerSecond / 1000.0
tokens = math.min(capacity, tokens + refilled)

local allowed = 0
if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
end

redis.call('HMSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(now))
-- 한동안 요청이 없으면 버킷 자체를 지운다 - 안 쓰는 클라이언트 IP의 버킷이 Redis에 영원히 남지 않게.
redis.call('EXPIRE', KEYS[1], 60)

return {allowed, tostring(tokens)}
