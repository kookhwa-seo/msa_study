package com.msastudy.vehicle.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.domain.VehicleStatus;
import com.msastudy.vehicle.outbox.OutboxEvent;
import com.msastudy.vehicle.outbox.OutboxEventRepository;
import com.msastudy.vehicle.repository.ProcessedEventRepository;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * 동시 예약 배정 방어 실습(docs/concurrent-booking-lab.md). 같은 차종+지점에 대한 가승인이
 * 동시에 N건 들어오는데 차량이 딱 1대뿐인 상황을 만들어서, 락 없음/낙관적 락/비관적 락 세
 * 방식이 실제로 몇 건을 중복 배정하는지(같은 vehicleId가 VehicleAssigned 이벤트 2건 이상에
 * 실리는 것) 실측한다.
 *
 * <p>실제 로컬 vehicle-db(Postgres)가 필요해서 {@code @Tag("integration")}으로 표시했고
 * 기본 {@code ./gradlew test}에서는 빠진다 — {@code ./gradlew :vehicle-service:concurrencyTest}로
 * 실행한다 (사전에 {@code docker compose up -d vehicle-db} 필요).
 */
@SpringBootTest(properties = {
        // pool이 동시 스레드 수보다 작으면 요청들이 커넥션을 놓고 줄을 서면서 사실상 순차
        // 실행돼버려 경합 자체가 재현되지 않는다 - 진짜 동시 커넥션을 보장하려고 넉넉히 늘렸다.
        "spring.datasource.hikari.maximum-pool-size=30"
})
@Tag("integration")
class VehicleAssignmentConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(VehicleAssignmentConcurrencyTest.class);
    private static final int CONCURRENT_REQUESTS = 20;
    private static final String TEST_BRANCH_ID = "9999999999";

    @Autowired
    private VehicleAssignmentService vehicleAssignmentService;
    @Autowired
    private VehicleRepository vehicleRepository;
    @Autowired
    private OutboxEventRepository outboxEventRepository;
    @Autowired
    private ProcessedEventRepository processedEventRepository;
    @Autowired
    private ObjectMapper objectMapper;

    private final List<String> vehicleIdsToCleanUp = new java.util.ArrayList<>();
    private final List<UUID> eventIdsToCleanUp = new java.util.ArrayList<>();
    private final List<String> testReservationIdPrefixes = new java.util.ArrayList<>();

    /**
     * 반드시 outbox_event까지 지워야 한다 - 이 테스트가 쓰는 vehicle-db는 실제로 로컬에서 계속
     * 떠 있는 vehicle-service 인스턴스와 같은 DB다. outbox_event를 안 지우면, 그 살아있는
     * 인스턴스의 OutboxPublisher(@Scheduled)가 이 테스트가 만든 가짜 VehicleAssigned/
     * VehicleAssignFailed를 실제 Kafka로 발행해버리고, payment-service가 존재하지 않는
     * reservationId/vehicleId를 처리하려다 poison message가 돼 재시도 backlog를 쌓는다
     * (실제로 한 번 이렇게 로컬 환경을 오염시킨 적이 있다).
     */
    @AfterEach
    void cleanUp() {
        vehicleRepository.deleteAllByIdInBatch(vehicleIdsToCleanUp);
        processedEventRepository.deleteAllByIdInBatch(eventIdsToCleanUp);
        List<OutboxEvent> testOutboxEvents = outboxEventRepository.findAll().stream()
                .filter(e -> testReservationIdPrefixes.stream().anyMatch(prefix -> e.getAggregateId().startsWith(prefix)))
                .toList();
        outboxEventRepository.deleteAll(testOutboxEvents);
        vehicleIdsToCleanUp.clear();
        eventIdsToCleanUp.clear();
        testReservationIdPrefixes.clear();
    }

    @Test
    void withoutLock_allowsDuplicateAssignment() throws Exception {
        long duplicateAssignments = run("without-lock", vehicleAssignmentService::handlePaymentAuthorizedWithoutLock);

        log.info("[without-lock] 성공적으로 배정된 건수(원래 1대뿐이라 1보다 크면 전부 중복 배정): {}",
                duplicateAssignments);
        // 락이 없으니 보통 1보다 큰 값이 나온다 - 다만 로컬 타이밍에 따라 우연히 0~1이 나올 수도
        // 있다는 점까지가 "락이 없으면 결과를 신뢰할 수 없다"는 이번 실습의 결론이다.
    }

    @Test
    void optimisticLock_neverDuplicatesAssignment() throws Exception {
        long successCount = run("optimistic-lock", vehicleAssignmentService::handlePaymentAuthorizedOptimistic);

        assertThat(successCount).isEqualTo(1);
    }

    @Test
    void pessimisticLock_neverDuplicatesAssignment() throws Exception {
        long successCount = run("pessimistic-lock", vehicleAssignmentService::handlePaymentAuthorized);

        assertThat(successCount).isEqualTo(1);
    }

    /**
     * 차량 1대를 seed하고, 같은 차종+지점을 놓고 경쟁하는 PaymentAuthorized N건을 거의 동시에
     * 발행한 뒤(CountDownLatch로 동시 출발), 실제로 VehicleAssigned가 몇 건 발행됐는지 센다.
     * 차량이 1대뿐이므로 이 값이 1을 넘으면 그만큼 중복 배정이 일어난 것이다.
     */
    private long run(String label, Consumer<PaymentAuthorizedEvent> handler) throws Exception {
        String runId = UUID.randomUUID().toString().substring(0, 8);
        testReservationIdPrefixes.add("CONC-" + runId);
        String vehicleId = "V-CONC-" + runId;
        vehicleIdsToCleanUp.add(vehicleId);
        vehicleRepository.save(Vehicle.of(vehicleId, VehicleType.COMPACT, TEST_BRANCH_ID));

        List<PaymentAuthorizedEvent> events = IntStream.range(0, CONCURRENT_REQUESTS)
                .mapToObj(i -> {
                    UUID eventId = UUID.randomUUID();
                    eventIdsToCleanUp.add(eventId);
                    String reservationId = "CONC-" + runId + "-" + i;
                    return new PaymentAuthorizedEvent(
                            eventId, Instant.now(), reservationId, 1,
                            reservationId, "PAY-" + reservationId, new BigDecimal("150000"),
                            VehicleType.COMPACT, TEST_BRANCH_ID);
                })
                .toList();

        CountDownLatch ready = new CountDownLatch(CONCURRENT_REQUESTS);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_REQUESTS);
        long elapsedMs;
        try {
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (PaymentAuthorizedEvent event : events) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    try {
                        handler.accept(event);
                    } catch (Exception e) {
                        log.debug("[{}] 요청 실패(경합에서 짐): {}", label, e.toString());
                    }
                }));
            }

            ready.await(10, TimeUnit.SECONDS);
            long start = System.nanoTime();
            go.countDown();
            for (java.util.concurrent.Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            elapsedMs = (System.nanoTime() - start) / 1_000_000;
        } finally {
            executor.shutdown();
        }
        log.info("[{}] {}건 동시 요청을 전부 처리하는 데 걸린 시간: {}ms", label, CONCURRENT_REQUESTS, elapsedMs);

        List<String> testReservationIds = events.stream().map(PaymentAuthorizedEvent::reservationId).toList();
        List<OutboxEvent> assignedEvents = outboxEventRepository.findAll().stream()
                .filter(e -> VehicleAssignedEvent.TYPE.equals(e.getEventType()))
                .filter(e -> testReservationIds.contains(e.getAggregateId()))
                .toList();

        List<String> assignedVehicleIds = assignedEvents.stream()
                .map(e -> readVehicleId(e))
                .collect(Collectors.toList());
        log.info("[{}] {}건 동시 요청 중 실제로 VehicleAssigned 발행된 건수: {} (배정된 vehicleId: {})",
                label, CONCURRENT_REQUESTS, assignedEvents.size(), assignedVehicleIds);

        return assignedEvents.size();
    }

    private String readVehicleId(OutboxEvent event) {
        try {
            return objectMapper.readValue(event.getPayload(), VehicleAssignedEvent.class).vehicleId();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
