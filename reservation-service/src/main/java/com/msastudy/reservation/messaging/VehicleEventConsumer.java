package com.msastudy.reservation.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
import com.msastudy.reservation.service.ReservationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * vehicle-events 토픽을 구독한다. 한 토픽에 여러 이벤트 타입이 섞여 오기 때문에
 * 라이브러리의 자동 타입 매핑에 기대지 않고, payload의 eventType 필드를 직접 읽어
 * 어떤 레코드로 역직렬화할지 명시적으로 분기한다 (학습 목적: 라우팅 로직을 눈에 보이게).
 *
 * VehicleAssignFailed는 직접 구독하지 않는다. payment-service가 가승인을 취소(보상)한 뒤
 * 발행하는 PaymentVoided로 간접 처리한다 (PaymentEventConsumer 참고).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class VehicleEventConsumer {

    private final ObjectMapper objectMapper;
    private final ReservationService reservationService;

    @KafkaListener(topics = Topics.VEHICLE_EVENTS)
    public void onMessage(String payload) throws Exception {
        JsonNode node = objectMapper.readTree(payload);
        String eventType = node.get("eventType").asText();

        switch (eventType) {
            case VehicleAssignedEvent.TYPE -> reservationService.handleVehicleAssigned(
                    objectMapper.readValue(payload, VehicleAssignedEvent.class));
            case VehicleReleasedEvent.TYPE -> reservationService.handleVehicleReleased(
                    objectMapper.readValue(payload, VehicleReleasedEvent.class));
            default -> log.debug("reservation-service가 처리하지 않는 이벤트 타입: {}", eventType);
        }
    }
}
