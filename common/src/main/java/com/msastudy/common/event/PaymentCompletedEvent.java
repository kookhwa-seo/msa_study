package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PaymentCompletedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String paymentId,
        BigDecimal amount,
        Instant paidAt
) implements DomainEvent {

    public static final String TYPE = "PaymentCompleted";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
