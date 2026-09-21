package com.msastudy.payment.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 가승인/매입 2단계 결제. 가승인은 카드 한도만 보류해두는 것이라 돈이 움직이지 않고,
 * 매입(capture)이 되어야 실제 청구가 확정된다. 매입 전에는 void로 보류만 해제할 수 있다.
 *
 * 예약 하나에는 결제 건이 정확히 하나여야 한다. reservation_id UK는 중복 가승인을 막는 DB 수준의
 * 마지막 방어선이다 — eventId 기반 멱등성(processed_event)은 "같은 이벤트의 재전달"만 막고,
 * 다른 eventId로 같은 예약이 다시 들어오거나 동시에 처리되는 경우는 이 제약이 막는다.
 */
@Entity
@Table(
        name = "payment",
        uniqueConstraints = @UniqueConstraint(name = "uk_payment_reservation_id", columnNames = "reservation_id")
)
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Payment {

    @Id
    private String id;

    private String reservationId;

    /** 매입 시점에(VehicleAssigned를 받았을 때) 채워진다. 가승인 단계에는 아직 배정 전이다. */
    private String vehicleId;

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    private Instant authorizedAt;

    /** 마지막 상태 전이 시각. CAPTURED일 때는 실제 결제 시각(paidAt)이 된다. */
    private Instant processedAt;

    public static Payment authorized(String reservationId, BigDecimal amount) {
        return of(reservationId, amount, PaymentStatus.AUTHORIZED);
    }

    public static Payment authFailed(String reservationId, BigDecimal amount) {
        return of(reservationId, amount, PaymentStatus.AUTH_FAILED);
    }

    private static Payment of(String reservationId, BigDecimal amount, PaymentStatus status) {
        Payment payment = new Payment();
        payment.id = UUID.randomUUID().toString();
        payment.reservationId = reservationId;
        payment.amount = amount;
        payment.status = status;
        payment.authorizedAt = Instant.now();
        payment.processedAt = payment.authorizedAt;
        return payment;
    }

    public boolean isAuthorizationExpired(Instant now, Duration ttl) {
        return authorizedAt.plus(ttl).isBefore(now);
    }

    public void capture(String vehicleId) {
        requireAuthorized("capture");
        this.vehicleId = vehicleId;
        transitionTo(PaymentStatus.CAPTURED);
    }

    public void expire(String vehicleId) {
        requireAuthorized("expire");
        this.vehicleId = vehicleId;
        transitionTo(PaymentStatus.EXPIRED);
    }

    public void voidAuthorization() {
        requireAuthorized("void");
        transitionTo(PaymentStatus.VOIDED);
    }

    private void requireAuthorized(String action) {
        if (status != PaymentStatus.AUTHORIZED) {
            throw new IllegalStateException(
                    "AUTHORIZED 상태에서만 " + action + " 가능: paymentId=" + id + ", status=" + status);
        }
    }

    private void transitionTo(PaymentStatus next) {
        this.status = next;
        this.processedAt = Instant.now();
    }
}
