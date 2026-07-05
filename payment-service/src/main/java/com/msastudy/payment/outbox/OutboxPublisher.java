package com.msastudy.payment.outbox;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * reservation-service/vehicle-service의 OutboxPublisher와 동작은 동일하지만, 각 서비스가
 * 자기 폴링 퍼블리셔를 직접 구현/소유한다 (학습 목적 — CLAUDE.md 참고).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publishPending() {
        List<OutboxEvent> pending = outboxEventRepository.findTop50ByPublishedAtIsNullOrderByCreatedAtAsc();

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
                event.markPublished(Instant.now());
                log.info("outbox published: topic={}, eventType={}, aggregateId={}",
                        event.getTopic(), event.getEventType(), event.getAggregateId());
            } catch (Exception e) {
                log.error("outbox publish failed, eventId={} (다음 폴링에서 재시도)", event.getId(), e);
                break;
            }
        }
    }
}
