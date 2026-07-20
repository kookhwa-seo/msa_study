package com.msastudy.reservation.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Kafka는 at-least-once 전달을 보장하므로 같은 이벤트가 재전달될 수 있다. 처리를
 * 끝낸 eventId를 기록해두고, 재전달된 이벤트는 비즈니스 로직을 다시 실행하지 않고
 * 건너뛴다 (idempotent consumer).
 */
@Entity
@Table(name = "processed_event")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEvent {

    @Id
    private UUID eventId;

    private Instant processedAt;

    public static ProcessedEvent of(UUID eventId) {
        ProcessedEvent processedEvent = new ProcessedEvent();
        processedEvent.eventId = eventId;
        processedEvent.processedAt = Instant.now();
        return processedEvent;
    }
}
