package com.msastudy.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record VehicleAssignedEvent(
        UUID eventId,
        Instant occurredAt,
        String aggregateId,
        int version,
        String reservationId,
        String vehicleId,
        String branchId
) implements DomainEvent {

    public static final String TYPE = "VehicleAssigned";

    @JsonProperty("eventType")
    @Override
    public String eventType() {
        return TYPE;
    }
}
