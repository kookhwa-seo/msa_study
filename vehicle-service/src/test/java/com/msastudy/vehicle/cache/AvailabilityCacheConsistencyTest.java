package com.msastudy.vehicle.cache;

import static org.assertj.core.api.Assertions.assertThat;

import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.repository.ProcessedEventRepository;
import com.msastudy.vehicle.repository.VehicleRepository;
import com.msastudy.vehicle.service.VehicleAssignmentService;
import com.msastudy.vehicle.web.BranchController;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 가용 차량 수 캐싱 정합성 실습(docs/availability-cache-lab.md). 실제 vehicle-db(Postgres) +
 * Redis가 필요해서 {@code @Tag("integration")}으로 표시했다 - {@code
 * ./gradlew :vehicle-service:concurrencyTest}로 실행한다(사전에 {@code docker compose up -d
 * vehicle-db redis} 필요).
 *
 * <p>확인하려는 것: (1) 같은 조회를 반복하면 두 번째부터는 DB를 안 건드리고 캐시만 보는지,
 * (2) 배정으로 실제 재고가 바뀐 뒤에도 무효화 없이 그냥 TTL만 믿으면 stale 값을 계속
 * 돌려주는지, (3) 이 프로젝트가 실제로 구현한 무효화가 그 stale 윈도우를 없애는지.
 */
@SpringBootTest
@Tag("integration")
class AvailabilityCacheConsistencyTest {

    @Autowired
    private BranchController branchController;
    @Autowired
    private VehicleRepository vehicleRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;
    @Autowired
    private AvailabilityCacheService availabilityCacheService;
    @Autowired
    private DbQueryCounter dbQueryCounter;
    @Autowired
    private VehicleAssignmentService vehicleAssignmentService;

    private String vehicleId;
    private String branchId;
    private UUID eventId;

    @AfterEach
    void cleanUp() {
        if (vehicleId != null) {
            vehicleRepository.deleteById(vehicleId);
        }
        if (eventId != null) {
            processedEventRepository.deleteById(eventId);
        }
        if (branchId != null) {
            availabilityCacheService.evict(VehicleType.COMPACT, branchId);
        }
    }

    @Test
    void secondLookupHitsCacheInsteadOfDb() {
        seedOneAvailableVehicle();

        Map<VehicleType, Long> first = branchController.stock(branchId);
        long dbQueriesAfterFirst = dbQueryCounter.get();
        Map<VehicleType, Long> second = branchController.stock(branchId);
        long dbQueriesAfterSecond = dbQueryCounter.get();

        assertThat(first.get(VehicleType.COMPACT)).isEqualTo(1L);
        assertThat(second.get(VehicleType.COMPACT)).isEqualTo(1L);
        // 두 번째 조회는 캐시 히트라 DB 쿼리 카운터가 늘어나지 않아야 한다.
        assertThat(dbQueriesAfterSecond).isEqualTo(dbQueriesAfterFirst);
    }

    @Test
    void assignmentInvalidatesCacheSoNextLookupSeesFreshCount() {
        seedOneAvailableVehicle();

        // 캐시를 채운다(COMPACT=1로 캐싱됨).
        Map<VehicleType, Long> beforeAssign = branchController.stock(branchId);
        assertThat(beforeAssign.get(VehicleType.COMPACT)).isEqualTo(1L);
        assertThat(availabilityCacheService.get(VehicleType.COMPACT, branchId)).hasValue(1L);

        // 실제 배정(프로덕션 경로) - 커밋 후 캐시가 무효화돼야 한다.
        eventId = UUID.randomUUID();
        PaymentAuthorizedEvent event = new PaymentAuthorizedEvent(
                eventId, Instant.now(), "CACHE-" + vehicleId, 1,
                "CACHE-" + vehicleId, "PAY-" + vehicleId, new BigDecimal("150000"),
                VehicleType.COMPACT, branchId);
        vehicleAssignmentService.handlePaymentAuthorized(event);

        // 무효화됐으므로 캐시엔 값이 없어야 한다 - TTL 만료를 기다리지 않고도.
        assertThat(availabilityCacheService.get(VehicleType.COMPACT, branchId)).isEmpty();

        // 다음 조회는 DB에서 새로 읽어야 하고, 배정으로 줄어든 0을 봐야 한다(캐시에 남아있던
        // stale 값인 1이 아니라).
        Map<VehicleType, Long> afterAssign = branchController.stock(branchId);
        assertThat(afterAssign.get(VehicleType.COMPACT)).isEqualTo(0L);
    }

    private void seedOneAvailableVehicle() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        vehicleId = "V-CACHE-" + suffix;
        // 법정동코드 자릿수(10자리)와 형태만 맞추면 되는 테스트 전용 branchId - 실제 카탈로그와
        // 겹치지 않게 8로 시작하는 임의 숫자열을 쓴다.
        branchId = "8" + String.format("%09d", System.nanoTime() % 1_000_000_000L);
        vehicleRepository.save(Vehicle.of(vehicleId, VehicleType.COMPACT, branchId));
        availabilityCacheService.evict(VehicleType.COMPACT, branchId);
    }
}
