package com.msastudy.reservation.web;

import com.msastudy.common.event.VehicleType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.time.Instant;

public record CreateReservationRequest(
        @NotBlank String customerId,
        @NotNull VehicleType vehicleType,
        @NotBlank String branchId,
        @NotNull Instant rentalStartAt,
        @NotNull Instant rentalEndAt,
        @NotNull @Positive BigDecimal totalAmount
) {
}
