package com.msastudy.vehicle.web;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.config.BranchCatalog;
import com.msastudy.vehicle.domain.Branch;
import com.msastudy.vehicle.domain.VehicleStatus;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 지점(법정동) 목록/재고 조회 API. 목록 조회는 프런트엔드가 이 응답 하나로 시/도 ->
 * 시/군/구 -> 동 3단계 드롭다운을 전부 구성한다(항목이 1,500여 개라 매 단계 별도 API를
 * 만드는 대신 한 번에 내려주고 클라이언트에서 그룹핑한다). 재고 조회는 사용자가 예약을
 * 만들기 전에 "이 지점에 이 차종이 실제로 있는지" 미리 볼 수 있게 하는 용도다 — 재고가
 * 없는 조합으로 예약해서 배정 실패(CANCELLED)로 끝나는 걸 미리 피하게 해준다.
 */
@RestController
@RequestMapping("/api/branches")
@RequiredArgsConstructor
public class BranchController {

    private final BranchCatalog branchCatalog;
    private final VehicleRepository vehicleRepository;

    @GetMapping
    public List<Branch> list() {
        return branchCatalog.getAll();
    }

    @GetMapping("/{branchId}/stock")
    public Map<VehicleType, Long> stock(@PathVariable String branchId) {
        Map<VehicleType, Long> stock = new LinkedHashMap<>();
        for (VehicleType type : VehicleType.values()) {
            stock.put(type, vehicleRepository.countByVehicleTypeAndBranchIdAndStatus(
                    type, branchId, VehicleStatus.AVAILABLE));
        }
        return stock;
    }
}
