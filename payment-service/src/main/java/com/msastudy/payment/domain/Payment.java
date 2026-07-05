package com.msastudy.payment.domain;

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
@Table(name = "payment")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment {

    @Id
    private String id;

    private String reservationId;

    private String vehicleId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    private Instant processedAt;

    public static Payment completed(String reservationId, String vehicleId, BigDecimal amount) {
        return of(reservationId, vehicleId, amount, PaymentStatus.COMPLETED);
    }

    public static Payment failed(String reservationId, String vehicleId, BigDecimal amount) {
        return of(reservationId, vehicleId, amount, PaymentStatus.FAILED);
    }

    private static Payment of(String reservationId, String vehicleId, BigDecimal amount, PaymentStatus status) {
        Payment payment = new Payment();
        payment.id = UUID.randomUUID().toString();
        payment.reservationId = reservationId;
        payment.vehicleId = vehicleId;
        payment.amount = amount;
        payment.status = status;
        payment.processedAt = Instant.now();
        return payment;
    }
}
