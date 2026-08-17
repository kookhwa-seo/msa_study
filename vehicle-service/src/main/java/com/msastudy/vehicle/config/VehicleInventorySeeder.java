package com.msastudy.vehicle.config;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Branch;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * {@link BranchCatalog}의 모든 지점(법정동)마다 차종별 0~3대를 무작위로 시딩한다.
 * 고정 시드(42)를 써서 재기동해도 항상 같은 재고 분포가 나오게 했다 — 일부 지점/차종은
 * 자연스럽게 0대가 나와서 "재고 없음" 시나리오가 별도 코드 없이도 재현된다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VehicleInventorySeeder implements CommandLineRunner {

    private static final int MAX_PER_TYPE = 3;
    private static final long RANDOM_SEED = 42L;

    private final VehicleRepository vehicleRepository;
    private final BranchCatalog branchCatalog;

    @Override
    public void run(String... args) {
        if (vehicleRepository.count() > 0) {
            return;
        }

        Random random = new Random(RANDOM_SEED);
        List<Vehicle> seed = new ArrayList<>();
        for (Branch branch : branchCatalog.getAll()) {
            for (VehicleType type : VehicleType.values()) {
                int count = random.nextInt(MAX_PER_TYPE + 1);
                for (int i = 1; i <= count; i++) {
                    String id = branch.code() + "-" + type.name() + "-" + i;
                    seed.add(Vehicle.of(id, type, branch.code()));
                }
            }
        }
        vehicleRepository.saveAll(seed);
        log.info("차량 재고 시드 완료: 지점 {}곳, 차량 {}대", branchCatalog.getAll().size(), seed.size());
    }
}
