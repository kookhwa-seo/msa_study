package com.msastudy.payment.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * reservation-events 토픽을 구독한다. payment-service는 결제에 필요한 금액 정보를
 * 얻기 위해 ReservationCreated를 구독해 로컬에 스냅샷을 저장해둔다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReservationEventConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(topics = Topics.RESERVATION_EVENTS)
    public void onMessage(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            String eventType = node.get("eventType").asText();

            if (ReservationCreatedEvent.TYPE.equals(eventType)) {
                paymentService.handleReservationCreated(
                        objectMapper.readValue(payload, ReservationCreatedEvent.class));
            } else {
                log.debug("payment-service가 처리하지 않는 이벤트 타입: {}", eventType);
            }
        } catch (Exception e) {
            log.error("reservation-events 처리 실패, payload={}", payload, e);
        }
    }
}
