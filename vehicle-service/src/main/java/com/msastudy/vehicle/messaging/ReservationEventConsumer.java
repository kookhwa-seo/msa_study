package com.msastudy.vehicle.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.vehicle.service.VehicleAssignmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * reservation-events 토픽을 구독한다. vehicle-service가 관심 있는 이벤트는
 * ReservationCreated뿐이라 나머지 타입(ReservationCancelled 등)은 무시한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReservationEventConsumer {

    private final ObjectMapper objectMapper;
    private final VehicleAssignmentService vehicleAssignmentService;

    @KafkaListener(topics = Topics.RESERVATION_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        if (ReservationCreatedEvent.TYPE.equals(eventType)) {
            vehicleAssignmentService.handleReservationCreated(
                    objectMapper.readValue(payload, ReservationCreatedEvent.class));
        } else {
            log.debug("vehicle-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
