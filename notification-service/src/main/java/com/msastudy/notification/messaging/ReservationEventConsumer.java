package com.msastudy.notification.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.ReservationCancelledEvent;
import com.msastudy.common.event.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * reservation-events 토픽을 구독한다. notification-service는 최종 취소 확정
 * (ReservationCancelled)만 관심 있다.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ReservationEventConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = Topics.RESERVATION_EVENTS)
    public void onMessage(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            String eventType = node.get("eventType").asText();

            if (ReservationCancelledEvent.TYPE.equals(eventType)) {
                ReservationCancelledEvent event = objectMapper.readValue(payload, ReservationCancelledEvent.class);
                log.info("[알림톡] 예약 {}가 취소되었습니다. (사유: {})",
                        event.reservationId(), event.reason());
            } else {
                log.debug("notification-service가 처리하지 않는 이벤트 타입: {}", eventType);
            }
        } catch (Exception e) {
            log.error("reservation-events 처리 실패, payload={}", payload, e);
        }
    }
}
