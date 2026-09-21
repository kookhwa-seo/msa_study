package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * 차량 배정 실패에 대한 보상: 가승인(한도 보류)을 취소했다는 이벤트. reservation-service는
 * VehicleAssignFailed에 직접 반응하지 않고, 이 보상 완료 이벤트를 받았을 때 예약을 최종 취소한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentVoidedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String paymentId
) implements DomainEvent {

    public static final String TYPE = "PaymentVoided";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
