package com.msastudy.reservation.web;

import com.msastudy.common.event.VehicleType;
import com.msastudy.reservation.domain.Reservation;
import com.msastudy.reservation.domain.ReservationStatus;
import java.math.BigDecimal;
import java.time.Instant;

public record ReservationResponse(
        String reservationId,
        String customerId,
        VehicleType vehicleType,
        String branchId,
        Instant rentalStartAt,
        Instant rentalEndAt,
        BigDecimal totalAmount,
        ReservationStatus status,
        Instant createdAt,
        Instant updatedAt
) {
    public static ReservationResponse from(Reservation reservation) {
        return new ReservationResponse(
                reservation.getId(),
                reservation.getCustomerId(),
                reservation.getVehicleType(),
                reservation.getBranchId(),
                reservation.getRentalStartAt(),
                reservation.getRentalEndAt(),
                reservation.getTotalAmount(),
                reservation.getStatus(),
                reservation.getCreatedAt(),
                reservation.getUpdatedAt()
        );
    }
}
