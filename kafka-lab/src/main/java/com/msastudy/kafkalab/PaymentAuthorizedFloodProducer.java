package com.msastudy.kafkalab;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleType;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 파티션 수 x 컨슈머 수에 따른 처리량 실습(docs/consumer-throughput-lab.md) 전용 도구.
 * vehicle-service가 실제로 구독하는 {@code payment-events} 토픽에 진짜
 * {@link PaymentAuthorizedEvent} 스키마 그대로 대량 발행하고, **같은 프로세스 안에서** Kafka
 * AdminClient로 vehicle-service 컨슈머 그룹의 lag을 직접 폴링해서 "전부 처리 완료"까지 걸린
 * 시간을 잰다. 처음에는 {@code kafka-consumer-groups.sh}를 셸에서 반복 호출하는 방식으로
 * 쟀는데, 그 스크립트 자체가 매번 새 JVM을 띄우는 탓에 호출 하나에 수 초가 걸려서 측정값이
 * 신뢰할 수 없었다 - 그래서 발행과 lag 폴링을 전부 이 프로세스 안으로 옮겼다.
 *
 * <p>branchId를 일부러 "9999999999"(실제 지점 카탈로그에 없는 코드)로 고정한 이유는
 * {@link #NONEXISTENT_BRANCH_ID} 주석 참고 - 배정 로직 자체가 아니라 순수 컨슈머 처리량만
 * 재려는 것이고, 실제 재고를 건드리지 않으며, 성공 이벤트가 하류(payment-service 등)로
 * 흘러가 poison message를 만드는 것도 피한다.
 *
 * <p>실행: {@code ./gradlew :kafka-lab:floodPaymentAuthorized --args="10000"}
 */
public final class PaymentAuthorizedFloodProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizedFloodProducer.class);
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String CONSUMER_GROUP = "vehicle-service";
    private static final String NONEXISTENT_BRANCH_ID = "9999999999";

    private PaymentAuthorizedFloodProducer() {
    }

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 10_000;

        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        Properties adminProps = new Properties();
        adminProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (Admin admin = Admin.create(adminProps)) {
            TopicPartition[] partitions = admin.describeTopics(List.of(Topics.PAYMENT_EVENTS))
                    .topicNameValues().get(Topics.PAYMENT_EVENTS).get()
                    .partitions().stream()
                    .map(p -> new TopicPartition(Topics.PAYMENT_EVENTS, p.partition()))
                    .toArray(TopicPartition[]::new);
            log.info("대상 토픽 {} 파티션 수: {}", Topics.PAYMENT_EVENTS, partitions.length);

            long baselineLag = totalLag(admin, partitions);
            if (baselineLag != 0) {
                log.warn("실험 시작 전 lag이 0이 아님(baseline={}) - 이전 실행 잔여분일 수 있다. " +
                        "결과 해석에 주의.", baselineLag);
            }

            long produceStartNanos = System.nanoTime();
            produce(objectMapper, count);
            long producedElapsedMs = (System.nanoTime() - produceStartNanos) / 1_000_000;
            log.info("발행 완료: {}건, {}ms (프로듀서 자체 속도, 병목 아님을 확인용)", count, producedElapsedMs);

            log.info("컨슈머({})가 다 처리할 때까지 lag을 추적한다...", CONSUMER_GROUP);
            long watchStart = System.nanoTime();
            long prevLag = -1;
            while (true) {
                long lag = totalLag(admin, partitions);
                long elapsedMs = (System.nanoTime() - watchStart) / 1_000_000;
                if (lag != prevLag) {
                    log.info("t+{}ms lag={}", elapsedMs, lag);
                    prevLag = lag;
                }
                if (lag <= 0) {
                    long totalElapsedMs = (System.nanoTime() - watchStart) / 1_000_000;
                    double throughput = count * 1000.0 / Math.max(totalElapsedMs, 1);
                    log.info("==================== 결과 ====================");
                    log.info("파티션 수={}, 처리 건수={}, 소진까지 걸린 시간={}ms, 처리량={} msg/s",
                            partitions.length, count, totalElapsedMs, String.format("%.1f", throughput));
                    break;
                }
                Thread.sleep(50);
            }
        }
    }

    private static long totalLag(Admin admin, TopicPartition[] partitions) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin
                .listConsumerGroupOffsets(CONSUMER_GROUP, new ListConsumerGroupOffsetsOptions())
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> latestRequest = new java.util.HashMap<>();
        for (TopicPartition tp : partitions) {
            latestRequest.put(tp, OffsetSpec.latest());
        }
        Map<TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                admin.listOffsets(latestRequest).all().get();

        long lag = 0;
        for (TopicPartition tp : partitions) {
            long end = endOffsets.get(tp).offset();
            OffsetAndMetadata committedOffset = committed.get(tp);
            long current = committedOffset != null ? committedOffset.offset() : 0;
            lag += Math.max(end - current, 0);
        }
        return lag;
    }

    private static void produce(ObjectMapper objectMapper, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);

        String runId = UUID.randomUUID().toString().substring(0, 8);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                String reservationId = "FLOOD-" + runId + "-" + i;
                PaymentAuthorizedEvent event = new PaymentAuthorizedEvent(
                        UUID.randomUUID(),
                        Instant.now(),
                        reservationId,
                        1,
                        reservationId,
                        "PAY-" + reservationId,
                        new BigDecimal("100000"),
                        VehicleType.COMPACT,
                        NONEXISTENT_BRANCH_ID
                );
                String payload;
                try {
                    payload = objectMapper.writeValueAsString(event);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                producer.send(new ProducerRecord<>(Topics.PAYMENT_EVENTS, reservationId, payload));
            }
            producer.flush();
        }
    }
}
