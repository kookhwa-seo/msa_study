package com.msastudy.reservation.ratelimit;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis 토큰 버킷 rate limiter. 클라이언트(IP)마다 독립된 버킷을 두고, 매 요청이 토큰 1개를
 * 소비한다. 버킷은 {@code capacity}만큼 순간적으로 몰리는 트래픽(버스트)을 허용하고, 그 뒤로는
 * 초당 {@code refillPerSecond}개씩만 다시 채워진다 - "짧은 순간 몰리는 건 어느 정도 받아주되,
 * 계속 그 속도로는 못 받는다"는 흔한 API rate limit 정책이다.
 *
 * <p>실제 판정 로직은 {@code token_bucket.lua} Lua 스크립트가 Redis 안에서 원자적으로
 * 실행한다 - 왜 애플리케이션 코드에서 직접 "읽고 계산하고 쓰기"를 하면 안 되는지는 그 스크립트
 * 주석 참고.
 */
@Component
@RequiredArgsConstructor
public class TokenBucketRateLimiter {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.rate-limit.capacity:50}")
    private int capacity;

    @Value("${app.rate-limit.refill-per-second:50}")
    private int refillPerSecond;

    private final DefaultRedisScript<List> script = loadScript();

    /** true면 허용(토큰 소비됨), false면 거절(429 대상). */
    public boolean tryConsume(String clientKey) {
        String bucketKey = "rate-limit:" + clientKey;
        @SuppressWarnings("unchecked")
        List<Object> result = redisTemplate.execute(
                script,
                List.of(bucketKey),
                String.valueOf(capacity),
                String.valueOf(refillPerSecond),
                String.valueOf(System.currentTimeMillis()),
                "1"
        );
        return result != null && "1".equals(String.valueOf(result.get(0)));
    }

    private static DefaultRedisScript<List> loadScript() {
        DefaultRedisScript<List> redisScript = new DefaultRedisScript<>();
        redisScript.setLocation(new ClassPathResource("scripts/token_bucket.lua"));
        redisScript.setResultType(List.class);
        return redisScript;
    }
}
