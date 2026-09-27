package com.msastudy.vehicle.cache;

import com.msastudy.common.event.VehicleType;
import java.time.Duration;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 가용 차량 수 캐싱 실습(docs/availability-cache-lab.md). {@code availability:{vehicleType}:
 * {branchId}} 키로 차종+지점별 가용 대수를 Redis에 짧게(TTL 5~30초) 캐싱한다 - DB 트랜잭션의
 * 영속성 컨텍스트(1급 캐시)와 달리, 이 캐시는 여러 요청·여러 인스턴스가 공유해서 본다는 점이
 * 핵심 차이다. 그래서 TTL로 자동 만료되는 것과 별개로, 실제 재고가 바뀌는 시점(배정/보상)에
 * 명시적으로 무효화해야 한다(안 그러면 배정 직후에도 방금 캐싱된 stale 값이 그대로 보인다) -
 * {@link com.msastudy.vehicle.service.VehicleAssignmentService}의 evict 호출 참고.
 */
@Component
public class AvailabilityCacheService {

    private final StringRedisTemplate redisTemplate;
    private final Duration ttl;

    public AvailabilityCacheService(
            StringRedisTemplate redisTemplate,
            @Value("${app.cache.availability-ttl-seconds:10}") long ttlSeconds
    ) {
        this.redisTemplate = redisTemplate;
        this.ttl = Duration.ofSeconds(ttlSeconds);
    }

    public Optional<Long> get(VehicleType vehicleType, String branchId) {
        String value = redisTemplate.opsForValue().get(key(vehicleType, branchId));
        return Optional.ofNullable(value).map(Long::parseLong);
    }

    public void put(VehicleType vehicleType, String branchId, long count) {
        redisTemplate.opsForValue().set(key(vehicleType, branchId), String.valueOf(count), ttl);
    }

    public void evict(VehicleType vehicleType, String branchId) {
        redisTemplate.delete(key(vehicleType, branchId));
    }

    private String key(VehicleType vehicleType, String branchId) {
        return "availability:" + vehicleType + ":" + branchId;
    }
}
