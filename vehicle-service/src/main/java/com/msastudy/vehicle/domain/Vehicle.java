package com.msastudy.vehicle.domain;

import com.msastudy.common.event.VehicleType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "vehicle")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Vehicle {

    @Id
    private String id;

    @Enumerated(EnumType.STRING)
    private VehicleType vehicleType;

    private String branchId;

    @Enumerated(EnumType.STRING)
    private VehicleStatus status;

    public static Vehicle of(String id, VehicleType vehicleType, String branchId) {
        Vehicle vehicle = new Vehicle();
        vehicle.id = id;
        vehicle.vehicleType = vehicleType;
        vehicle.branchId = branchId;
        vehicle.status = VehicleStatus.AVAILABLE;
        return vehicle;
    }

    public void assign() {
        this.status = VehicleStatus.ASSIGNED;
    }

    public void release() {
        this.status = VehicleStatus.AVAILABLE;
    }
}
