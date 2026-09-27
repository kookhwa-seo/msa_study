package com.msastudy.vehicle.cache;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/**
 * 캐싱 전/후 DB 쿼리 수를 실측 비교하기 위한 실습 전용 카운터(docs/availability-cache-lab.md).
 * 캐시 미스로 실제 재고 COUNT 쿼리가 나갈 때만 증가한다 - {@link com.msastudy.vehicle.web.
 * BranchController}의 디버그 엔드포인트로 조회/리셋한다.
 */
@Component
public class DbQueryCounter {

    private final AtomicLong count = new AtomicLong();

    public void increment() {
        count.incrementAndGet();
    }

    public long get() {
        return count.get();
    }

    public void reset() {
        count.set(0);
    }
}
