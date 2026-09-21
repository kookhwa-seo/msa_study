package com.msastudy.vehicle.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.vehicle.service.VehicleAssignmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * payment-events 토픽을 구독한다. vehicle-service는 reservation-events를 구독하지 않는다 —
 * 가승인이 성공한(PaymentAuthorized) 예약에만 차량을 배정하고, 매입 단계에서 실패한
 * (PaymentFailed) 경우 배정했던 차량을 재고로 되돌리는 보상 트랜잭션을 수행한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;
    private final VehicleAssignmentService vehicleAssignmentService;

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        switch (eventType) {
            case PaymentAuthorizedEvent.TYPE -> vehicleAssignmentService.handlePaymentAuthorized(
                    objectMapper.readValue(payload, PaymentAuthorizedEvent.class));
            case PaymentFailedEvent.TYPE -> vehicleAssignmentService.handlePaymentFailed(
                    objectMapper.readValue(payload, PaymentFailedEvent.class));
            default -> log.debug("vehicle-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
