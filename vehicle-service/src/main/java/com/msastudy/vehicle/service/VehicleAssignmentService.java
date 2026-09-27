package com.msastudy.vehicle.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
import com.msastudy.vehicle.cache.AvailabilityCacheService;
import com.msastudy.vehicle.domain.ProcessedEvent;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.domain.VehicleStatus;
import com.msastudy.vehicle.outbox.OutboxEvent;
import com.msastudy.vehicle.outbox.OutboxEventRepository;
import com.msastudy.vehicle.repository.ProcessedEventRepository;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 동시 배정 방어 실습(docs/concurrent-booking-lab.md): 같은 차종+지점에 대한 가승인이 동시에
 * 여러 건 들어오면, "재고 확인"과 "배정 표시" 사이에 다른 트랜잭션이 끼어들어 같은 물리적 차량이
 * 서로 다른 두 예약에 동시에 배정될 수 있다(check-then-act 경합). 이 클래스는 그 경합을 막는
 * 방식 세 가지를 나란히 둔다 - 실제로 Kafka 리스너가 부르는 건 {@link #handlePaymentAuthorized}
 * (비관적 락) 하나뿐이고, 나머지 둘은 비교 실습/테스트 전용이다.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class VehicleAssignmentService {

    private static final int SCHEMA_VERSION = 1;

    private final VehicleRepository vehicleRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final AvailabilityCacheService availabilityCacheService;

    /**
     * 프로덕션 경로. 가승인이 성공한 예약에 대해서만 재고 배정을 시도한다 (결제 안 된 주문이
     * 차량을 잡지 않는다). {@link VehicleRepository#lockFirstAvailable}로 조건에 맞는 차량
     * 딱 1행을 {@code SELECT ... FOR UPDATE}로 잠근다 - 동시에 들어온 다른 요청은 이 조회
     * 자체에서 대기하게 되므로, 같은 차량이 두 예약에 동시에 배정되는 경합이 원천적으로
     * 불가능하다. 재고 배정 시도 + outbox 저장을 하나의 트랜잭션으로 묶는다.
     *
     * Kafka는 at-least-once 전달이라 같은 PaymentAuthorized가 재전달될 수 있다. eventId가
     * 이미 처리된 적 있으면 재배정을 시도하지 않고 건너뛴다.
     */
    @Transactional
    public void handlePaymentAuthorized(PaymentAuthorizedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("이미 처리한 이벤트, 스킵: eventId={}, type={}", event.eventId(), PaymentAuthorizedEvent.TYPE);
            return;
        }

        Optional<Vehicle> available = vehicleRepository
                .lockFirstAvailable(event.vehicleType(), event.branchId(), VehicleStatus.AVAILABLE, PageRequest.of(0, 1))
                .stream()
                .findFirst();

        if (available.isEmpty()) {
            publishAssignFailed(event);
        } else {
            Vehicle vehicle = available.get();
            vehicle.assign();
            evictAvailabilityAfterCommit(vehicle);
            publishAssigned(event, vehicle);
        }

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    /**
     * 비교/버그 재현 전용 - 실제 리스너는 이 메서드를 부르지 않는다. 원래(락 도입 전) 코드
     * 그대로: "조회"와 "배정 표시" 사이에 락이 없어서, 동시에 들어온 요청 여럿이 같은 AVAILABLE
     * 행을 동시에 읽고 각자 assign()해버릴 수 있다 - 같은 차량이 여러 예약에 배정되는 버그.
     *
     * 조회와 저장 사이에 일부러 짧게 sleep을 넣었다 - 실제 코드에는 당연히 없는 지연이지만,
     * 로컬 Postgres에서는 조회~커밋이 보통 1ms도 안 걸려서 경합 창이 너무 좁아 20개 스레드를
     * 동시에 던져도 우연히 안 겹칠 때가 있다(실제로 처음 돌렸을 때 그랬다). 이 지연은 "실제
     * 재고 차감 로직이 그만큼 걸린다"를 흉내내는 게 아니라, 눈에 보이는 경합을 안정적으로
     * 재현하기 위한 실습용 장치다.
     *
     * 저장은 {@code vehicle.assign()} + 더티체킹이 아니라
     * {@link VehicleRepository#updateStatusIgnoringVersion}(벌크 UPDATE)을 쓴다 - {@code
     * Vehicle}에 {@code @Version}이 있어서, 엔티티를 통한 평범한 저장은 Hibernate가 자동으로
     * 버전 체크를 걸어버려 의도한 "락이 전혀 없는 상태"를 재현할 수 없기 때문.
     */
    @Transactional
    void handlePaymentAuthorizedWithoutLock(PaymentAuthorizedEvent event) {
        Optional<Vehicle> available = vehicleRepository.findFirstByVehicleTypeAndBranchIdAndStatus(
                event.vehicleType(), event.branchId(), VehicleStatus.AVAILABLE);

        sleepToWidenRaceWindow();

        if (available.isEmpty()) {
            publishAssignFailed(event);
        } else {
            Vehicle vehicle = available.get();
            vehicleRepository.updateStatusIgnoringVersion(vehicle.getId(), VehicleStatus.ASSIGNED);
            publishAssigned(event, vehicle);
        }

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    /**
     * 비교 전용 - 낙관적 락(@Version) 버전. 비관적 락처럼 미리 잠그지 않고, 대신 읽은 시점의
     * 버전과 저장 시점의 버전이 다르면(그 사이 다른 트랜잭션이 먼저 같은 행을 바꿨다는 뜻)
     * {@link ObjectOptimisticLockingFailureException}이 터진다. 이 예외는 보통 flush/commit
     * 시점에 나서 트랜잭션이 이미 rollback-only가 된 뒤라, PaymentService의 UK 처리와 같은
     * 이유로 {@link TransactionTemplate}을 써서 트랜잭션 경계를 메서드 안쪽으로 좁히고
     * 롤백이 끝난 바깥에서 처리한다. 경합에서 진 쪽은 "배정 실패"로 취급한다(재시도하지 않음 -
     * 재시도까지 하면 순수한 두 락 방식의 비교가 아니게 된다).
     */
    void handlePaymentAuthorizedOptimistic(PaymentAuthorizedEvent event) {
        boolean assigned;
        try {
            // 성공 경로는 조회 -> 배정 -> saveAndFlush(여기서 버전 충돌이 터짐) -> 이벤트 발행 ->
            // 처리 완료 표시까지 전부 한 트랜잭션 안에서 끝낸다. saveAndFlush에서 예외가 나면
            // 이 트랜잭션 전체가 롤백되므로 부분 커밋 걱정이 없다.
            assigned = Boolean.TRUE.equals(transactionTemplate.execute(status -> {
                Optional<Vehicle> available = vehicleRepository.findFirstByVehicleTypeAndBranchIdAndStatus(
                        event.vehicleType(), event.branchId(), VehicleStatus.AVAILABLE);
                if (available.isEmpty()) {
                    return false;
                }
                Vehicle vehicle = available.get();
                vehicle.assign();
                vehicleRepository.saveAndFlush(vehicle);
                publishAssigned(event, vehicle);
                processedEventRepository.save(ProcessedEvent.of(event.eventId()));
                return true;
            }));
        } catch (ObjectOptimisticLockingFailureException e) {
            log.warn("낙관적 락 충돌 - 다른 트랜잭션이 먼저 같은 차량을 배정함: reservationId={}",
                    event.reservationId());
            assigned = false;
        }

        // 재고 없음(assigned=false, 예외 없음) 또는 낙관적 락 충돌(assigned=false, catch에서 옴)
        // 둘 다 별도의 새 트랜잭션으로 실패 이벤트를 발행한다 - 충돌로 롤백된 트랜잭션 안에서는
        // 아무것도 커밋할 수 없기 때문에 반드시 새 트랜잭션이어야 한다.
        if (!assigned) {
            transactionTemplate.executeWithoutResult(status -> {
                publishAssignFailed(event);
                processedEventRepository.save(ProcessedEvent.of(event.eventId()));
            });
        }
    }

    /**
     * 매입 단계 결제 실패(예: 가승인 만료)에 대한 보상 트랜잭션: 배정했던 차량을 다시 재고로 돌리고, 그 결과를
     * VehicleReleased 이벤트로 발행한다 (reservation-service가 이를 받아 최종 취소 처리).
     */
    @Transactional
    public void handlePaymentFailed(PaymentFailedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("이미 처리한 이벤트, 스킵: eventId={}, type={}", event.eventId(), PaymentFailedEvent.TYPE);
            return;
        }

        Vehicle vehicle = vehicleRepository.findById(event.vehicleId())
                .orElseThrow(() -> new NoSuchElementException("존재하지 않는 차량: " + event.vehicleId()));
        vehicle.release();
        evictAvailabilityAfterCommit(vehicle);

        VehicleReleasedEvent released = new VehicleReleasedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                vehicle.getId()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.VEHICLE_EVENTS, event.reservationId(), VehicleReleasedEvent.TYPE, writeJson(released)));

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    private void publishAssigned(PaymentAuthorizedEvent event, Vehicle vehicle) {
        VehicleAssignedEvent assigned = new VehicleAssignedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                vehicle.getId(),
                vehicle.getBranchId()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.VEHICLE_EVENTS, event.reservationId(), VehicleAssignedEvent.TYPE, writeJson(assigned)));
    }

    private void publishAssignFailed(PaymentAuthorizedEvent event) {
        VehicleAssignFailedEvent failed = new VehicleAssignFailedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                VehicleAssignFailedEvent.Reason.OUT_OF_STOCK
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.VEHICLE_EVENTS, event.reservationId(), VehicleAssignFailedEvent.TYPE, writeJson(failed)));
    }

    private String writeJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("이벤트 직렬화 실패: " + event, e);
        }
    }

    /**
     * 커밋 이후에 무효화한다 - 트랜잭션이 아직 안 끝난 시점에 바로 evict하면, 그 사이(evict
     * 직후 ~ 실제 commit 전)에 다른 요청이 재고 조회를 해서 캐시를 "아직 커밋 전이라 값이
     * 그대로인" 값으로 다시 채워버릴 수 있다 - 그러면 진짜 commit이 끝난 뒤에도 캐시엔 방금
     * evict로 지운 의미가 없어진 stale 값이 남는다(다음 TTL 만료까지). afterCommit 콜백으로
     * 미루면 이 race가 없다: commit이 실제로 끝난 뒤에야 evict하므로, 그 이후의 재조회는
     * 항상 새 값을 본다.
     */
    private void evictAvailabilityAfterCommit(Vehicle vehicle) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            // 실제 트랜잭션 프록시 없이 서비스 메서드를 직접 호출하는 단위 테스트 등 - 커밋
            // 시점이라는 개념 자체가 없으니 즉시 지운다.
            availabilityCacheService.evict(vehicle.getVehicleType(), vehicle.getBranchId());
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                availabilityCacheService.evict(vehicle.getVehicleType(), vehicle.getBranchId());
            }
        });
    }

    private void sleepToWidenRaceWindow() {
        try {
            Thread.sleep(30);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
