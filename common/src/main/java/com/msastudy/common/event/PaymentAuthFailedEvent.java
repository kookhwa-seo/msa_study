package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

/**
 * 가승인 실패. 아직 차량을 배정하기 전이라 되돌릴 것이 없으므로 보상 단계 없이
 * reservation-service가 바로 예약을 취소한다.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentAuthFailedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        Reason reason
) implements DomainEvent {

    public static final String TYPE = "PaymentAuthFailed";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }

    public enum Reason {
        CARD_DECLINED,
        INSUFFICIENT_LIMIT,
        GATEWAY_TIMEOUT
    }
}
