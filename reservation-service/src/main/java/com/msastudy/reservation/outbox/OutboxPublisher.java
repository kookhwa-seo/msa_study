package com.msastudy.reservation.outbox;

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
 * outbox_event 테이블을 주기적으로 폴링해 아직 발행하지 않은 이벤트를 Kafka로 보낸다.
 * Debezium 같은 CDC 도구 없이 직접 구현한 폴링 방식 (학습 목적: 왜 Outbox가 필요한지,
 * 폴링 퍼블리셔가 어떻게 동작하는지 코드로 직접 확인).
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
                // 순서를 지키기 위해 실패한 이벤트 이후는 이번 폴링에서 중단하고 다음 주기에 재시도한다.
                break;
            }
        }
    }
}
