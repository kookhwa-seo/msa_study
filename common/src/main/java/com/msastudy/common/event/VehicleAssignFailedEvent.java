package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VehicleAssignFailedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        Reason reason
) implements DomainEvent {

    public static final String TYPE = "VehicleAssignFailed";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }

    public enum Reason {
        OUT_OF_STOCK,
        INVALID_VEHICLE_TYPE
    }
}
