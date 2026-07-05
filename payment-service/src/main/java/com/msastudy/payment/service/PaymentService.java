package com.msastudy.payment.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.payment.domain.Payment;
import com.msastudy.payment.domain.ReservationSnapshot;
import com.msastudy.payment.outbox.OutboxEvent;
import com.msastudy.payment.outbox.OutboxEventRepository;
import com.msastudy.payment.repository.PaymentRepository;
import com.msastudy.payment.repository.ReservationSnapshotRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class PaymentService {

    private static final int SCHEMA_VERSION = 1;

    private final ReservationSnapshotRepository reservationSnapshotRepository;
    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final BigDecimal failAboveAmount;

    public PaymentService(
            ReservationSnapshotRepository reservationSnapshotRepository,
            PaymentRepository paymentRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper,
            @Value("${app.payment.fail-above-amount}") BigDecimal failAboveAmount
    ) {
        this.reservationSnapshotRepository = reservationSnapshotRepository;
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.failAboveAmount = failAboveAmount;
    }

    /**
     * reservation-service를 동기 호출하지 않고, ReservationCreated 이벤트에서 결제에
     * 필요한 정보만 복사해 로컬에 보관해둔다. 실제 결제 시도는 VehicleAssigned를 받았을 때다.
     */
    @Transactional
    public void handleReservationCreated(ReservationCreatedEvent event) {
        reservationSnapshotRepository.save(ReservationSnapshot.of(
                event.reservationId(), event.customerId(), event.vehicleType(), event.totalAmount()));
    }

    /**
     * 결제 시도 + outbox 저장을 하나의 트랜잭션으로 묶는다. 금액이 임계값(app.payment.fail-above-amount)을
     * 넘으면 실패로 시뮬레이션한다 (실제 PG 연동 대신 학습용 결정 규칙).
     */
    @Transactional
    public void handleVehicleAssigned(VehicleAssignedEvent event) {
        ReservationSnapshot snapshot = reservationSnapshotRepository.findById(event.reservationId())
                .orElseThrow(() -> new NoSuchElementException("결제 대상 예약 정보 없음: " + event.reservationId()));

        if (snapshot.getTotalAmount().compareTo(failAboveAmount) > 0) {
            publishFailed(event, snapshot);
        } else {
            publishCompleted(event, snapshot);
        }
    }

    private void publishCompleted(VehicleAssignedEvent event, ReservationSnapshot snapshot) {
        Payment payment = Payment.completed(event.reservationId(), event.vehicleId(), snapshot.getTotalAmount());
        paymentRepository.save(payment);

        PaymentCompletedEvent completed = new PaymentCompletedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                payment.getId(),
                payment.getAmount(),
                payment.getProcessedAt()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentCompletedEvent.TYPE, writeJson(completed)));
    }

    private void publishFailed(VehicleAssignedEvent event, ReservationSnapshot snapshot) {
        Payment payment = Payment.failed(event.reservationId(), event.vehicleId(), snapshot.getTotalAmount());
        paymentRepository.save(payment);

        PaymentFailedEvent failed = new PaymentFailedEvent(
                UUID.randomUUID(),
                Instant.now(),
                event.reservationId(),
                SCHEMA_VERSION,
                event.reservationId(),
                event.vehicleId(),
                PaymentFailedEvent.Reason.INSUFFICIENT_LIMIT
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.PAYMENT_EVENTS, event.reservationId(), PaymentFailedEvent.TYPE, writeJson(failed)));
    }

    private String writeJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("이벤트 직렬화 실패: " + event, e);
        }
    }
}
