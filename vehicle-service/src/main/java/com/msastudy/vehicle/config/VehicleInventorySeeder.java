package com.msastudy.vehicle.config;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * 스터디용 초기 차량 재고 시드 데이터. 테이블이 비어 있을 때만 삽입한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VehicleInventorySeeder implements CommandLineRunner {

    private static final String BRANCH_ID = "SEOUL_GANGNAM";

    private final VehicleRepository vehicleRepository;

    @Override
    public void run(String... args) {
        if (vehicleRepository.count() > 0) {
            return;
        }

        List<Vehicle> seed = List.of(
                Vehicle.of("V-COMPACT-1", VehicleType.COMPACT, BRANCH_ID),
                Vehicle.of("V-COMPACT-2", VehicleType.COMPACT, BRANCH_ID),
                Vehicle.of("V-SUV-1", VehicleType.SUV, BRANCH_ID),
                Vehicle.of("V-VAN-1", VehicleType.VAN, BRANCH_ID)
        );
        vehicleRepository.saveAll(seed);
        log.info("차량 재고 시드 완료: {}대", seed.size());
    }
}
