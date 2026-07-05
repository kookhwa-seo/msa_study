package com.msastudy.reservation.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 비즈니스 데이터 저장과 같은 DB 트랜잭션 안에서 함께 저장되는 이벤트 레코드.
 * {@link OutboxPublisher}가 이 테이블을 폴링해 Kafka로 발행한다 (이중 쓰기 문제 해결).
 * 다른 서비스(vehicle-service)의 outbox 구현과 구조가 같아 보이지만, 서비스별로 각자
 * 소유하는 별개의 클래스/테이블이다 (common으로 추상화하지 않음).
 */
@Entity
@Table(name = "outbox_event")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent {

    @Id
    private UUID id;

    private String topic;

    private String aggregateId;

    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private Instant createdAt;

    private Instant publishedAt;

    public static OutboxEvent of(String topic, String aggregateId, String eventType, String payload) {
        OutboxEvent event = new OutboxEvent();
        event.id = UUID.randomUUID();
        event.topic = topic;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.payload = payload;
        event.createdAt = Instant.now();
        return event;
    }

    public void markPublished(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }
}
