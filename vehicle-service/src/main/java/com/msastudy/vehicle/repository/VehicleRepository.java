package com.msastudy.vehicle.repository;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.domain.VehicleStatus;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface VehicleRepository extends JpaRepository<Vehicle, String> {

    Optional<Vehicle> findFirstByVehicleTypeAndBranchIdAndStatus(
            VehicleType vehicleType, String branchId, VehicleStatus status);
}
