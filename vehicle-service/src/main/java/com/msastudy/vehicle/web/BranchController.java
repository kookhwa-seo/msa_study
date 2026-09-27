package com.msastudy.vehicle.web;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.cache.AvailabilityCacheService;
import com.msastudy.vehicle.cache.DbQueryCounter;
import com.msastudy.vehicle.config.BranchCatalog;
import com.msastudy.vehicle.domain.Branch;
import com.msastudy.vehicle.domain.VehicleStatus;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 지점(법정동) 목록/재고 조회 API. 목록 조회는 프런트엔드가 이 응답 하나로 시/도 ->
 * 시/군/구 -> 동 3단계 드롭다운을 전부 구성한다(항목이 1,500여 개라 매 단계 별도 API를
 * 만드는 대신 한 번에 내려주고 클라이언트에서 그룹핑한다). 재고 조회는 사용자가 예약을
 * 만들기 전에 "이 지점에 이 차종이 실제로 있는지" 미리 볼 수 있게 하는 용도다 — 재고가
 * 없는 조합으로 예약해서 배정 실패(CANCELLED)로 끝나는 걸 미리 피하게 해준다.
 *
 * <p>재고 조회는 Redis 캐시(cache-aside)를 거친다: 캐시에 있으면 그대로 반환하고, 없으면
 * DB를 조회해 채운 뒤 반환한다(docs/availability-cache-lab.md). 실제 재고가 바뀌는 시점
 * (배정/보상)의 무효화는 {@link com.msastudy.vehicle.service.VehicleAssignmentService}가 한다.
 * {@code app.cache.enabled}(기본 true)를 끄면 매번 DB를 직접 조회한다 - 적용 전/후 비교
 * 실습 전용 토글(`docs/rate-limiting-lab.md`의 `app.rate-limit.enabled`와 같은 용도).
 */
@RestController
@RequestMapping("/api/branches")
public class BranchController {

    private final BranchCatalog branchCatalog;
    private final VehicleRepository vehicleRepository;
    private final AvailabilityCacheService availabilityCacheService;
    private final DbQueryCounter dbQueryCounter;
    private final boolean cacheEnabled;

    public BranchController(
            BranchCatalog branchCatalog,
            VehicleRepository vehicleRepository,
            AvailabilityCacheService availabilityCacheService,
            DbQueryCounter dbQueryCounter,
            @Value("${app.cache.enabled:true}") boolean cacheEnabled
    ) {
        this.branchCatalog = branchCatalog;
        this.vehicleRepository = vehicleRepository;
        this.availabilityCacheService = availabilityCacheService;
        this.dbQueryCounter = dbQueryCounter;
        this.cacheEnabled = cacheEnabled;
    }

    @GetMapping
    public List<Branch> list() {
        return branchCatalog.getAll();
    }

    @GetMapping("/{branchId}/stock")
    public Map<VehicleType, Long> stock(@PathVariable String branchId) {
        Map<VehicleType, Long> stock = new LinkedHashMap<>();
        for (VehicleType type : VehicleType.values()) {
            Optional<Long> cached = cacheEnabled ? availabilityCacheService.get(type, branchId) : Optional.empty();
            long count = cached.orElseGet(() -> loadAndCache(type, branchId));
            stock.put(type, count);
        }
        return stock;
    }

    /** 캐싱 전/후 DB 쿼리 수 비교 실습 전용 디버그 엔드포인트 (docs/availability-cache-lab.md). */
    @GetMapping("/debug/db-query-count")
    public long dbQueryCount() {
        return dbQueryCounter.get();
    }

    @PostMapping("/debug/db-query-count/reset")
    public void resetDbQueryCount() {
        dbQueryCounter.reset();
    }

    private long loadAndCache(VehicleType type, String branchId) {
        long fresh = vehicleRepository.countByVehicleTypeAndBranchIdAndStatus(
                type, branchId, VehicleStatus.AVAILABLE);
        dbQueryCounter.increment();
        if (cacheEnabled) {
            availabilityCacheService.put(type, branchId, fresh);
        }
        return fresh;
    }
}
