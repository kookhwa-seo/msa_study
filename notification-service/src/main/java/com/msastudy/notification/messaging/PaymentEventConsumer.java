package com.msastudy.notification.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * payment-events 토픽을 구독해 결제 결과를 알림(카카오톡 알림톡 흉내)으로 흘려보낸다.
 * notification-service는 자체 DB가 없고 멱등성도 중요하지 않아 Outbox 패턴을 적용하지
 * 않는다 (CLAUDE.md 참고) — 그냥 로그로 시뮬레이션한다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = Topics.PAYMENT_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        switch (eventType) {
            case PaymentCompletedEvent.TYPE -> {
                PaymentCompletedEvent event = objectMapper.readValue(payload, PaymentCompletedEvent.class);
                log.info("[알림톡] 예약 {} 결제가 완료되었습니다. (결제ID: {}, 금액: {})",
                        event.reservationId(), event.paymentId(), event.amount());
            }
            case PaymentFailedEvent.TYPE -> {
                PaymentFailedEvent event = objectMapper.readValue(payload, PaymentFailedEvent.class);
                log.info("[알림톡] 예약 {} 결제가 실패했습니다. (사유: {})",
                        event.reservationId(), event.reason());
            }
            default -> log.debug("notification-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
