package com.msastudy.kafkalab;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 순서 보장 실습.
 *
 * <p>이 프로젝트의 실제 Saga 이벤트(ReservationCreated 등)는 항상 reservationId를 key로
 * 써서 같은 예약의 이벤트가 같은 파티션으로 가게 만든다({@code docs/event-schema.md} 참고).
 * 이 랩은 그 규칙을 "지켰을 때"와 "어겼을 때"를 파티션 3개짜리 토픽에서 직접 재현해서,
 * 왜 그 규칙이 필요한지를 로그로 확인한다.
 *
 * <ul>
 *   <li>{@code broken} 모드: 같은 aggregate(AGG-1)의 이벤트 20개를 key 없이, 파티션을
 *       0,1,2로 명시적으로 돌려가며 보낸다 (round-robin으로 부하를 분산시키는 실수를
 *       그대로 재현). Spring {@code @KafkaListener concurrency=3}과 동일하게 컨슈머
 *       그룹 멤버 3개가 파티션을 하나씩 맡아 "동시에" 처리하므로, 처리 완료 순서가
 *       전송 순서와 어긋난다.</li>
 *   <li>{@code fixed} 모드: 같은 20개를 key=AGG-1로 보낸다. 기본 파티셔너가 key를 해시해
 *       항상 같은 파티션으로 보내므로, 그 파티션을 맡은 컨슈머 그룹 멤버 1개만 순차
 *       처리하고 나머지 2개는 유휴 상태가 된다. 처리 완료 순서가 전송 순서와 정확히
 *       일치한다.</li>
 * </ul>
 *
 * 실행: {@code ./gradlew :kafka-lab:run --args="broken"} 또는 {@code --args="fixed"}
 * (docker-compose의 kafka가 떠 있어야 한다).
 */
public final class OrderingLabRunner {

    private static final Logger log = LoggerFactory.getLogger(OrderingLabRunner.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "ordering-lab-events";
    private static final int PARTITIONS = 3;
    private static final String AGGREGATE_ID = "AGG-1";
    private static final int MESSAGE_COUNT = 20;

    private OrderingLabRunner() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "broken";
        if (!mode.equals("broken") && !mode.equals("fixed")) {
            throw new IllegalArgumentException("mode는 broken 또는 fixed여야 한다: " + mode);
        }

        ensureTopic();

        String groupId = "ordering-lab-" + mode + "-" + System.currentTimeMillis();
        ConcurrentLinkedQueue<ProcessedRecord> processed = new ConcurrentLinkedQueue<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch consumersReady = new CountDownLatch(PARTITIONS);

        List<Thread> consumerThreads = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            String consumerName = "consumer-" + i;
            Thread t = new Thread(() -> runConsumer(groupId, consumerName, processed, stop, consumersReady),
                    consumerName);
            t.setDaemon(true);
            consumerThreads.add(t);
            t.start();
        }

        // 컨슈머 그룹이 파티션 배정을 마칠 때까지 대기한다 (join 전에 produce하면
        // auto.offset.reset=latest 특성상 메시지를 놓친다).
        consumersReady.await(15, TimeUnit.SECONDS);
        Thread.sleep(1000);

        produce(mode);

        // 20개가 다 처리되거나 타임아웃(처리 지연 시뮬레이션 최대 200ms * 20개를 감안해 여유)될 때까지 대기.
        long deadline = System.currentTimeMillis() + 15_000;
        while (processed.size() < MESSAGE_COUNT && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        stop.set(true);
        for (Thread t : consumerThreads) {
            t.join(5_000);
        }

        report(mode, processed);
    }

    private static void ensureTopic() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        try (Admin admin = Admin.create(props)) {
            NewTopic newTopic = new NewTopic(TOPIC, PARTITIONS, (short) 1);
            try {
                admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
                log.info("topic created: {} (partitions={})", TOPIC, PARTITIONS);
            } catch (Exception e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.info("topic already exists: {}", TOPIC);
                } else {
                    throw e;
                }
            }
        }
    }

    private static void produce(String mode) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                final int seq = i;
                ProducerRecord<String, String> record;
                if (mode.equals("broken")) {
                    // key 없이 파티션을 직접 돌려가며 보낸다 - "부하 분산"을 위해
                    // round-robin을 손으로 구현하는 흔한 실수를 재현.
                    int partition = seq % PARTITIONS;
                    record = new ProducerRecord<>(TOPIC, partition, null, String.valueOf(seq));
                } else {
                    // key=AGGREGATE_ID -> 기본 파티셔너가 항상 같은 파티션으로 보낸다.
                    record = new ProducerRecord<>(TOPIC, AGGREGATE_ID, String.valueOf(seq));
                }
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("send failed seq={}", seq, exception);
                    } else {
                        log.info("sent seq={} -> partition={}, offset={}", seq, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
    }

    private static void runConsumer(String groupId, String consumerName, ConcurrentLinkedQueue<ProcessedRecord> processed,
            AtomicBoolean stop, CountDownLatch consumersReady) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerName);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            // 그룹 join(파티션 배정)이 안정될 때까지 반복 poll. 멤버 3개가 거의 동시에
            // join하면 서로의 join 때문에 리밸런스가 몇 차례 연쇄로 일어나므로, 짧은
            // poll 한 번으로는 최종 배정이 끝났다고 보장할 수 없다.
            // 브로커의 group.initial.rebalance.delay.ms(기본 3000ms)가 "그룹의 첫
            // JoinGroup 수신 시점"부터 카운트되므로, 여유 있게 잡는다.
            long warmupDeadline = System.currentTimeMillis() + 6000;
            while (System.currentTimeMillis() < warmupDeadline) {
                consumer.poll(Duration.ofMillis(200));
            }
            // auto.offset.reset=latest는 "언제 파티션을 처음 fetch하느냐"에 따라 latest가
            // 다르게 해석되는 레이스가 있다 (리밸런스가 produce보다 늦게 끝나면 이미 보낸
            // 메시지를 건너뛴다). seekToEnd + position()으로 "지금 시점" 오프셋을 produce
            // 이전에 못박아서 이 레이스를 없앤다.
            consumer.seekToEnd(consumer.assignment());
            for (var tp : consumer.assignment()) {
                consumer.position(tp);
            }
            log.info("[{}] warmup 완료, produce 시작 가능", consumerName);
            consumersReady.countDown();

            while (!stop.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    // 실제 리스너가 하는 작업(재고 차감, 결제 호출 등)의 처리 시간 편차를
                    // 흉내낸다. 이 편차가 있어야 "동시에 여러 파티션을 처리할 때 완료
                    // 순서가 전송 순서와 어긋난다"는 현상이 로그에서 뚜렷하게 보인다.
                    sleepRandomMillis(20, 180);
                    int seq = Integer.parseInt(record.value());
                    long processedAt = System.nanoTime();
                    processed.add(new ProcessedRecord(seq, record.partition(), consumerName, processedAt));
                    log.info("[{}] processed seq={} (partition={}, offset={})",
                            consumerName, seq, record.partition(), record.offset());
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }

    private static void sleepRandomMillis(int minInclusive, int maxInclusive) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextInt(minInclusive, maxInclusive + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void report(String mode, ConcurrentLinkedQueue<ProcessedRecord> processed) {
        List<ProcessedRecord> byCompletionTime = new ArrayList<>(processed);
        byCompletionTime.sort(Comparator.comparingLong(r -> r.processedAtNanos));

        StringBuilder completionOrder = new StringBuilder();
        int inversions = 0;
        int lastSeq = -1;
        for (ProcessedRecord r : byCompletionTime) {
            completionOrder.append(r.seq).append(' ');
            if (r.seq < lastSeq) {
                inversions++;
            }
            lastSeq = r.seq;
        }

        log.info("==================== 결과 ({} mode) ====================", mode);
        log.info("전송 순서 : 0 1 2 ... {} (오름차순)", MESSAGE_COUNT - 1);
        log.info("처리 완료 순서: {}", completionOrder.toString().trim());
        log.info("역전(순서가 깨진) 지점 수: {}", inversions);
        if (inversions == 0) {
            log.info("결론: 처리 완료 순서가 전송 순서와 일치한다 - 같은 key로 같은 파티션에 몰아넣고" +
                    " 파티션당 컨슈머 스레드 1개가 순차 처리했기 때문.");
        } else {
            log.info("결론: key 없이 여러 파티션에 흩뿌렸기 때문에, 파티션마다 별도 컨슈머 스레드가" +
                    " '동시에' 처리하면서 처리 시간 편차만큼 순서가 뒤섞였다. Kafka는 파티션" +
                    " 내부의 순서만 보장하고 파티션 간 순서는 보장하지 않는다.");
        }
    }

    private record ProcessedRecord(int seq, int partition, String consumerName, long processedAtNanos) {
    }
}
