package com.msastudy.payment.domain;

import com.msastudy.common.event.VehicleType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * payment-service가 결제를 시도할 때 필요한 예약 정보를 자기 DB에 보관하는 로컬 read
 * model. reservation-service를 동기 호출로 조회하지 않고, ReservationCreated 이벤트를
 * 구독해 필요한 필드만 복사해둔다 (Choreography Saga에서 흔한 패턴 — 서비스는 자기가
 * 필요한 데이터를 이벤트로 전달받아 로컬에 보관하지, 다른 서비스에 동기 조회하지 않는다).
 */
@Entity
@Table(name = "reservation_snapshot")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ReservationSnapshot {

    @Id
    private String reservationId;

    private String customerId;

    @Enumerated(EnumType.STRING)
    private VehicleType vehicleType;

    private BigDecimal totalAmount;

    public static ReservationSnapshot of(
            String reservationId, String customerId, VehicleType vehicleType, BigDecimal totalAmount) {
        ReservationSnapshot snapshot = new ReservationSnapshot();
        snapshot.reservationId = reservationId;
        snapshot.customerId = customerId;
        snapshot.vehicleType = vehicleType;
        snapshot.totalAmount = totalAmount;
        return snapshot;
    }
}
