package com.msastudy.reservation.domain;

import com.msastudy.common.event.VehicleType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "reservation")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Reservation {

    @Id
    private String id;

    private String customerId;

    @Enumerated(EnumType.STRING)
    private VehicleType vehicleType;

    private String branchId;

    private Instant rentalStartAt;

    private Instant rentalEndAt;

    private BigDecimal totalAmount;

    @Enumerated(EnumType.STRING)
    private ReservationStatus status;

    private Instant createdAt;

    private Instant updatedAt;

    public static Reservation create(
            String customerId,
            VehicleType vehicleType,
            String branchId,
            Instant rentalStartAt,
            Instant rentalEndAt,
            BigDecimal totalAmount
    ) {
        Reservation reservation = new Reservation();
        reservation.id = UUID.randomUUID().toString();
        reservation.customerId = customerId;
        reservation.vehicleType = vehicleType;
        reservation.branchId = branchId;
        reservation.rentalStartAt = rentalStartAt;
        reservation.rentalEndAt = rentalEndAt;
        reservation.totalAmount = totalAmount;
        reservation.status = ReservationStatus.PENDING;
        reservation.createdAt = Instant.now();
        reservation.updatedAt = reservation.createdAt;
        return reservation;
    }

    public void markPaymentPending() {
        this.status = ReservationStatus.PAYMENT_PENDING;
        this.updatedAt = Instant.now();
    }

    public void confirm() {
        this.status = ReservationStatus.CONFIRMED;
        this.updatedAt = Instant.now();
    }

    public void cancel() {
        this.status = ReservationStatus.CANCELLED;
        this.updatedAt = Instant.now();
    }
}
