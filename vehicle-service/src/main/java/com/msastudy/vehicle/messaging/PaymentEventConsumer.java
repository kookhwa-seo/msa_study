package com.msastudy.vehicle.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.vehicle.service.VehicleAssignmentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * payment-events 토픽을 구독한다. vehicle-service는 결제 실패(PaymentFailed)만 관심
 * 있다 — 배정했던 차량을 재고로 되돌리는 보상 트랜잭션을 트리거한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;
    private final VehicleAssignmentService vehicleAssignmentService;

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onMessage(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            String eventType = node.get("eventType").asText();

            if (PaymentFailedEvent.TYPE.equals(eventType)) {
                vehicleAssignmentService.handlePaymentFailed(
                        objectMapper.readValue(payload, PaymentFailedEvent.class));
            } else {
                log.debug("vehicle-service가 처리하지 않는 이벤트 타입: {}", eventType);
            }
        } catch (Exception e) {
            log.error("payment-events 처리 실패, payload={}", payload, e);
        }
    }
}
