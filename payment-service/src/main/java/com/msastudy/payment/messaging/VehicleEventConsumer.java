package com.msastudy.payment.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * vehicle-events 토픽을 구독한다. 차량 배정이 성공하면(VehicleAssigned) 가승인을 매입(capture)하고,
 * 배정이 실패하면(VehicleAssignFailed) 가승인을 취소(void)한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VehicleEventConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(topics = Topics.VEHICLE_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        switch (eventType) {
            case VehicleAssignedEvent.TYPE -> paymentService.handleVehicleAssigned(
                    objectMapper.readValue(payload, VehicleAssignedEvent.class));
            case VehicleAssignFailedEvent.TYPE -> paymentService.handleVehicleAssignFailed(
                    objectMapper.readValue(payload, VehicleAssignFailedEvent.class));
            default -> log.debug("payment-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
