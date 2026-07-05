package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ReservationCreatedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String customerId,
        VehicleType vehicleType,
        String branchId,
        Instant rentalStartAt,
        Instant rentalEndAt,
        BigDecimal totalAmount
) implements DomainEvent {

    public static final String TYPE = "ReservationCreated";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
