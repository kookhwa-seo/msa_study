package com.msastudy.reservation.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentAuthFailedEvent;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentVoidedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.reservation.service.ReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * payment-events 토픽을 구독한다. 예약의 최종 상태는 결제 쪽 결과로 갈린다.
 * - PaymentCompleted: 매입까지 끝남 → CONFIRMED
 * - PaymentAuthFailed: 가승인 거절. 배정 전이라 보상 없이 바로 CANCELLED
 * - PaymentVoided: 재고 부족으로 가승인 취소(보상) 완료 → CANCELLED
 *
 * 매입 단계 실패(PaymentFailed)는 직접 구독하지 않는다. vehicle-service의 배정 취소 보상이
 * 끝난 뒤 발행하는 VehicleReleased로 간접 처리한다 (VehicleEventConsumer 참고).
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

        switch (eventType) {
            case PaymentCompletedEvent.TYPE -> reservationService.handlePaymentCompleted(
                    objectMapper.readValue(payload, PaymentCompletedEvent.class));
            case PaymentAuthFailedEvent.TYPE -> reservationService.handlePaymentAuthFailed(
                    objectMapper.readValue(payload, PaymentAuthFailedEvent.class));
            case PaymentVoidedEvent.TYPE -> reservationService.handlePaymentVoided(
                    objectMapper.readValue(payload, PaymentVoidedEvent.class));
            default -> log.debug("reservation-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
