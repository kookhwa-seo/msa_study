package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 가승인(한도 보류) 성공. 다음 단계인 차량 배정에 필요한 정보(vehicleType, branchId)를
 * 이 이벤트에 그대로 실어 보낸다 — vehicle-service가 reservation-events를 따로 구독해
 * 로컬에 복사해둘 필요가 없고, 이벤트 간 도착 순서에 의존하지 않게 된다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentAuthorizedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String paymentId,
        BigDecimal amount,
        VehicleType vehicleType,
        String branchId
) implements DomainEvent {

    public static final String TYPE = "PaymentAuthorized";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
