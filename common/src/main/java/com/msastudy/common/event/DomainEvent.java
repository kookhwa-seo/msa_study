package com.msastudy.common.event;

import java.time.Instant;
import java.util.UUID;

/**
 * 모든 Saga 이벤트가 공통으로 갖는 봉투(envelope) 필드.
 * Kafka 메시지 key는 항상 aggregateId(reservationId)를 사용해 같은 예약에 대한
 * 이벤트 순서를 파티션 단위로 보장한다.
 */
public interface DomainEvent {

    UUID eventId();

    String eventType();

    Instant occurredAt();

    String aggregateId();

    int version();
}
