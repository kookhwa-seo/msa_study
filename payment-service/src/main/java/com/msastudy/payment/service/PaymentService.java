package com.msastudy.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentAuthFailedEvent;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.PaymentVoidedEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.payment.domain.Payment;
import com.msastudy.payment.domain.ProcessedEvent;
import com.msastudy.payment.outbox.OutboxEvent;
import com.msastudy.payment.outbox.OutboxEventRepository;
import com.msastudy.payment.repository.PaymentRepository;
import com.msastudy.payment.repository.ProcessedEventRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 가승인(authorize) → 매입(capture) 2단계 결제.
 *
 * <ul>
 *   <li>ReservationCreated → 가승인: 한도만 보류. 성공해야 차량 배정 단계로 넘어간다.</li>
 *   <li>VehicleAssigned → 매입: 배정이 확정된 뒤에야 실제 청구를 확정한다.</li>
 *   <li>VehicleAssignFailed → 승인 취소(void): 돈이 움직이지 않았으므로 보류만 해제한다.</li>
 * </ul>
 *
 * 결제 안 된 주문이 차량을 잡는 일이 없고, 재고 부족 시 보상이 환불이 아니라 void 한 줄이다.
 */
@Service
@Slf4j
public class PaymentService {

    private static final int SCHEMA_VERSION = 1;

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;
    private final BigDecimal failAboveAmount;
    private final Duration authorizationTtl;
    private final TransactionTemplate transactionTemplate;

    public PaymentService(
            PaymentRepository paymentRepository,
            OutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            ObjectMapper objectMapper,
            @Value("${app.payment.fail-above-amount}") BigDecimal failAboveAmount,
            @Value("${app.payment.authorization-ttl}") Duration authorizationTtl,
            TransactionTemplate transactionTemplate
    ) {
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
        this.failAboveAmount = failAboveAmount;
        this.authorizationTtl = authorizationTtl;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * 가승인 시도 + outbox 저장을 하나의 트랜잭션으로 묶는다. 금액이 임계값(app.payment.fail-above-amount)을
     * 넘으면 한도 부족으로 승인이 거절되는 것으로 시뮬레이션한다 (실제 PG/카드사 연동 대신 학습용 결정 규칙).
     *
     * 다음 단계(차량 배정)에 필요한 vehicleType/branchId를 PaymentAuthorized에 실어 보내므로,
     * vehicle-service는 reservation-events를 구독하지 않아도 되고 이벤트 도착 순서에 의존하지 않는다.
     *
     * 중복 가승인 방어는 두 겹이다.
     * 1) authorize() 안의 existsByReservationId — 순차적으로 들어온 중복을 예외 없이 스킵한다.
     * 2) payment.reservation_id UK — 위 확인과 저장 사이에 다른 트랜잭션이 끼어드는 동시 처리를 DB가 차단한다.
     *
     * 이 메서드에 @Transactional을 붙이지 않고 TransactionTemplate을 쓰는 이유: 유니크 위반은
     * 보통 INSERT가 flush되는 커밋 시점에 터지는데, 트랜잭션 안에서 예외를 잡으면 (1) 이미 rollback-only로
     * 표시돼 이어서 아무것도 할 수 없고 (2) 커밋 시점 예외는 애초에 메서드 안에서 잡히지도 않는다.
     * 그래서 트랜잭션 경계를 메서드 안쪽으로 좁히고, 롤백이 끝난 바깥에서 예외를 처리한다.
     */
    public void handleReservationCreated(ReservationCreatedEvent event) {
        try {
            transactionTemplate.executeWithoutResult(status -> authorize(event));
        } catch (DataIntegrityViolationException e) {
            // UK(payment.reservation_id) 또는 processed_event PK 위반. 트랜잭션은 이미 롤백돼
            // Payment/outbox가 남지 않았다. 그 예약의 결제가 실제로 존재할 때만 중복으로 보고 무시하고,
            // 아니면 다른 무결성 오류(예: NOT NULL 위반)이므로 그대로 던져 재시도/DLT로 넘긴다.
            if (!paymentRepository.existsByReservationId(event.reservationId())) {
                throw e;
            }
            log.warn("동시 처리로 인한 중복 가승인을 UK가 차단, 무시: reservationId={}, eventId={}",
                    event.reservationId(), event.eventId());
        }
    }

    private void authorize(ReservationCreatedEvent event) {
        if (alreadyProcessed(event.eventId(), ReservationCreatedEvent.TYPE)) {
            return;
        }

        if (paymentRepository.existsByReservationId(event.reservationId())) {
            // 다른 eventId로 같은 예약이 다시 들어온 경우. 새 결제를 만들거나 이벤트를 또 발행하지 않는다.
            log.warn("이미 결제 건이 있는 예약, 가승인 스킵: reservationId={}, eventId={}",
                    event.reservationId(), event.eventId());
        } else if (event.totalAmount().compareTo(failAboveAmount) > 0) {
            publishAuthFailed(event);
        } else {
            publishAuthorized(event);
        }

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    /**
     * 차량 배정이 확정됐으니 가승인을 매입(capture)한다. 승인 유효기간이 이미 지났다면 매입할 수 없어
     * PaymentFailed(AUTHORIZATION_EXPIRED)를 발행하고, vehicle-service가 배정을 되돌리는 보상을 수행한다.
     *
     * 가승인은 VehicleAssigned보다 인과적으로 앞서 저장되므로(PaymentAuthorized와 같은 트랜잭션),
     * Payment를 못 찾는다면 순서 문제가 아니라 데이터 이상이라 예외로 처리한다.
     */
    @Transactional
    public void handleVehicleAssigned(VehicleAssignedEvent event) {
        if (alreadyProcessed(event.eventId(), VehicleAssignedEvent.TYPE)) {
            return;
        }

        Payment payment = getOrThrow(event.reservationId());

        if (payment.isAuthorizationExpired(Instant.now(), authorizationTtl)) {
            payment.expire(event.vehicleId());
            publishFailed(event, PaymentFailedEvent.Reason.AUTHORIZATION_EXPIRED);
        } else {
            payment.capture(event.vehicleId());
            publishCompleted(payment);
        }

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    /**
     * 차량 배정 실패에 대한 보상: 가승인을 취소(void)한다. 돈이 움직이기 전이라 환불이 아니라
     * 보류 해제일 뿐이다. reservation-service는 이 보상이 끝났다는 PaymentVoided에 반응해 예약을 취소한다.
     */
    @Transactional
    public void handleVehicleAssignFailed(VehicleAssignFailedEvent event) {
        if (alreadyProcessed(event.eventId(), VehicleAssignFailedEvent.TYPE)) {
            return;
        }

        Payment payment = getOrThrow(event.reservationId());
        payment.voidAuthorization();

        PaymentVoidedEvent voided = new PaymentVoidedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                payment.getId()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentVoidedEvent.TYPE, writeJson(voided)));

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    private void publishAuthorized(ReservationCreatedEvent event) {
        Payment payment = Payment.authorized(event.reservationId(), event.totalAmount());
        paymentRepository.save(payment);

        PaymentAuthorizedEvent authorized = new PaymentAuthorizedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                payment.getId(),
                payment.getAmount(),
                event.vehicleType(),
                event.branchId()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentAuthorizedEvent.TYPE, writeJson(authorized)));
    }

    private void publishAuthFailed(ReservationCreatedEvent event) {
        paymentRepository.save(Payment.authFailed(event.reservationId(), event.totalAmount()));

        PaymentAuthFailedEvent failed = new PaymentAuthFailedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                PaymentAuthFailedEvent.Reason.INSUFFICIENT_LIMIT
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentAuthFailedEvent.TYPE, writeJson(failed)));
    }

    private void publishCompleted(Payment payment) {
        PaymentCompletedEvent completed = new PaymentCompletedEvent(
                UUID.randomUUID(),
                Instant.now(),
                payment.getReservationId(),
                SCHEMA_VERSION,
                payment.getReservationId(),
                payment.getId(),
                payment.getAmount(),
                payment.getProcessedAt()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, payment.getReservationId(), PaymentCompletedEvent.TYPE, writeJson(completed)));
    }

    private void publishFailed(VehicleAssignedEvent event, PaymentFailedEvent.Reason reason) {
        PaymentFailedEvent failed = new PaymentFailedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                event.vehicleId(),
                reason
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentFailedEvent.TYPE, writeJson(failed)));
    }

    /**
     * Kafka는 at-least-once 전달이라 같은 이벤트가 재전달될 수 있다. 이미 처리한 eventId면
     * 다시 처리하지 않는다 — 그렇지 않으면 가승인/매입/취소가 중복 수행되고 이벤트도 중복 발행된다.
     */
    private boolean alreadyProcessed(UUID eventId, String eventType) {
        if (processedEventRepository.existsById(eventId)) {
            log.info("이미 처리한 이벤트, 스킵: eventId={}, type={}", eventId, eventType);
            return true;
        }
        return false;
    }

    private Payment getOrThrow(String reservationId) {
        return paymentRepository.findByReservationId(reservationId)
                .orElseThrow(() -> new NoSuchElementException("가승인 내역 없음: " + reservationId));
    }

    private String writeJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("이벤트 직렬화 실패: " + event, e);
        }
    }
}
