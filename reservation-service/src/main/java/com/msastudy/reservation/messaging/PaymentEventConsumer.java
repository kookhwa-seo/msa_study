package com.msastudy.reservation.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.reservation.service.ReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * payment-events 토픽을 구독한다. reservation-service는 결제 성공(PaymentCompleted)만
 * 관심 있고, 결제 실패(PaymentFailed)는 vehicle-service의 보상(배정 취소) 완료 후
 * 발행하는 VehicleReleased를 통해 간접적으로 처리한다 (VehicleEventConsumer 참고).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;
    private final ReservationService reservationService;

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        if (PaymentCompletedEvent.TYPE.equals(eventType)) {
            reservationService.handlePaymentCompleted(
                    objectMapper.readValue(payload, PaymentCompletedEvent.class));
        } else {
            log.debug("reservation-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
