package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VehicleReleasedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String vehicleId
) implements DomainEvent {

    public static final String TYPE = "VehicleReleased";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
