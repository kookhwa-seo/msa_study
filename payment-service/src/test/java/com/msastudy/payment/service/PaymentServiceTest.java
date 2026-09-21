package com.msastudy.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.msastudy.common.event.PaymentAuthFailedEvent;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.PaymentVoidedEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.payment.domain.Payment;
import com.msastudy.payment.domain.PaymentStatus;
import com.msastudy.payment.outbox.OutboxEvent;
import com.msastudy.payment.outbox.OutboxEventRepository;
import com.msastudy.payment.repository.PaymentRepository;
import com.msastudy.payment.repository.ProcessedEventRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class PaymentServiceTest {

    private static final BigDecimal FAIL_ABOVE_AMOUNT = new BigDecimal("1000000");
    private static final Duration AUTHORIZATION_TTL = Duration.ofMinutes(30);
    /** 음수 TTL이면 방금 만든 가승인도 항상 만료된 것으로 취급된다 (시간에 의존하지 않는 만료 재현). */
    private static final Duration ALREADY_EXPIRED_TTL = Duration.ofSeconds(-1);

    private PaymentRepository paymentRepository;
    private OutboxEventRepository outboxEventRepository;
    private ProcessedEventRepository processedEventRepository;
    private ObjectMapper objectMapper;
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentRepository = mock(PaymentRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        processedEventRepository = mock(ProcessedEventRepository.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        paymentService = serviceWithTtl(AUTHORIZATION_TTL);
    }

    private PaymentService serviceWithTtl(Duration ttl) {
        return new PaymentService(
                paymentRepository, outboxEventRepository, processedEventRepository,
                objectMapper, FAIL_ABOVE_AMOUNT, ttl,
                // 트랜잭션 동작 자체는 여기서 검증하지 않는다 — 콜백을 그대로 실행하고 예외를 바깥으로 흘려주면 충분하다.
                new TransactionTemplate(mock(PlatformTransactionManager.class)));
    }

    private ReservationCreatedEvent reservationCreated(String reservationId, VehicleType type, String amount) {
        return new ReservationCreatedEvent(
                UUID.randomUUID(), Instant.now(), reservationId, 1,
                reservationId, "CUST-1", type, "SEOUL_GANGNAM",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal(amount));
    }

    private VehicleAssignedEvent vehicleAssigned(String reservationId, String vehicleId) {
        return new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), reservationId, 1, reservationId, vehicleId, "SEOUL_GANGNAM");
    }

    private OutboxEvent capturedOutboxEvent() {
        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        return captor.getValue();
    }

    @Test
    void handleReservationCreated_authorizesAndPublishesPaymentAuthorizedWithVehicleInfo() throws Exception {
        paymentService.handleReservationCreated(reservationCreated("R-1", VehicleType.COMPACT, "150000"));

        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(paymentCaptor.capture());
        assertThat(paymentCaptor.getValue().getStatus()).isEqualTo(PaymentStatus.AUTHORIZED);

        OutboxEvent outboxEvent = capturedOutboxEvent();
        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentAuthorizedEvent.TYPE);
        assertThat(outboxEvent.getAggregateId()).isEqualTo("R-1");

        PaymentAuthorizedEvent authorized =
                objectMapper.readValue(outboxEvent.getPayload(), PaymentAuthorizedEvent.class);
        assertThat(authorized.reservationId()).isEqualTo("R-1");
        assertThat(authorized.amount()).isEqualByComparingTo("150000");
        // 다음 단계(차량 배정)가 필요로 하는 값이 이벤트에 실려 있어야 한다.
        assertThat(authorized.vehicleType()).isEqualTo(VehicleType.COMPACT);
        assertThat(authorized.branchId()).isEqualTo("SEOUL_GANGNAM");
    }

    @Test
    void handleReservationCreated_publishesAuthFailedWhenAmountExceedsLimit() throws Exception {
        paymentService.handleReservationCreated(reservationCreated("R-2", VehicleType.SUV, "2000000"));

        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
        verify(paymentRepository).save(paymentCaptor.capture());
        assertThat(paymentCaptor.getValue().getStatus()).isEqualTo(PaymentStatus.AUTH_FAILED);

        OutboxEvent outboxEvent = capturedOutboxEvent();
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentAuthFailedEvent.TYPE);

        PaymentAuthFailedEvent failed = objectMapper.readValue(outboxEvent.getPayload(), PaymentAuthFailedEvent.class);
        assertThat(failed.reservationId()).isEqualTo("R-2");
        assertThat(failed.reason()).isEqualTo(PaymentAuthFailedEvent.Reason.INSUFFICIENT_LIMIT);
    }

    @Test
    void handleReservationCreated_skipsAlreadyProcessedEvent() {
        ReservationCreatedEvent event = reservationCreated("R-1", VehicleType.COMPACT, "150000");
        when(processedEventRepository.existsById(event.eventId())).thenReturn(true);

        paymentService.handleReservationCreated(event);

        verify(paymentRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void handleReservationCreated_skipsWhenPaymentAlreadyExistsForReservation() {
        // 다른 eventId로 같은 예약이 다시 들어온 경우 (eventId 기반 멱등성으로는 못 거른다).
        ReservationCreatedEvent event = reservationCreated("R-1", VehicleType.COMPACT, "150000");
        when(paymentRepository.existsByReservationId("R-1")).thenReturn(true);

        paymentService.handleReservationCreated(event);

        verify(paymentRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
        // 이벤트 자체는 소비한 것이므로 재전달 시 다시 처리하지 않도록 처리 이력은 남긴다.
        verify(processedEventRepository).save(any());
    }

    @Test
    void handleReservationCreated_ignoresUniqueViolationWhenPaymentAlreadyExists() {
        // 존재 확인 시점엔 없었지만(false) 저장 시점에 다른 트랜잭션이 먼저 커밋해 UK가 막은 경우.
        // 예외가 난 뒤 다시 조회하면 그 결제가 보인다(true) → 중복으로 보고 삼킨다.
        ReservationCreatedEvent event = reservationCreated("R-1", VehicleType.COMPACT, "150000");
        when(paymentRepository.existsByReservationId("R-1")).thenReturn(false, true);
        when(paymentRepository.save(any())).thenThrow(new DataIntegrityViolationException("uk_payment_reservation_id"));

        paymentService.handleReservationCreated(event); // 예외가 전파되지 않아야 한다
    }

    @Test
    void handleReservationCreated_rethrowsIntegrityViolationWhenNoPaymentExists() {
        // 예외 뒤에도 그 예약의 결제가 없다 = 중복이 아닌 다른 무결성 오류. 삼키면 안 되고 재시도/DLT로 넘겨야 한다.
        ReservationCreatedEvent event = reservationCreated("R-1", VehicleType.COMPACT, "150000");
        when(paymentRepository.existsByReservationId("R-1")).thenReturn(false);
        when(paymentRepository.save(any())).thenThrow(new DataIntegrityViolationException("not null violation"));

        assertThatThrownBy(() -> paymentService.handleReservationCreated(event))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    void handleVehicleAssigned_capturesAuthorizationAndPublishesPaymentCompleted() throws Exception {
        Payment payment = Payment.authorized("R-1", new BigDecimal("150000"));
        when(paymentRepository.findByReservationId("R-1")).thenReturn(Optional.of(payment));

        paymentService.handleVehicleAssigned(vehicleAssigned("R-1", "V-1"));

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(payment.getVehicleId()).isEqualTo("V-1");

        OutboxEvent outboxEvent = capturedOutboxEvent();
        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentCompletedEvent.TYPE);

        PaymentCompletedEvent completed = objectMapper.readValue(outboxEvent.getPayload(), PaymentCompletedEvent.class);
        assertThat(completed.reservationId()).isEqualTo("R-1");
        assertThat(completed.paymentId()).isEqualTo(payment.getId());
        assertThat(completed.amount()).isEqualByComparingTo("150000");
    }

    @Test
    void handleVehicleAssigned_publishesPaymentFailedWhenAuthorizationExpired() throws Exception {
        Payment payment = Payment.authorized("R-3", new BigDecimal("150000"));
        when(paymentRepository.findByReservationId("R-3")).thenReturn(Optional.of(payment));

        serviceWithTtl(ALREADY_EXPIRED_TTL).handleVehicleAssigned(vehicleAssigned("R-3", "V-3"));

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.EXPIRED);

        OutboxEvent outboxEvent = capturedOutboxEvent();
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentFailedEvent.TYPE);

        PaymentFailedEvent failed = objectMapper.readValue(outboxEvent.getPayload(), PaymentFailedEvent.class);
        assertThat(failed.reservationId()).isEqualTo("R-3");
        // vehicle-service가 어떤 차량을 되돌려야 하는지 알 수 있도록 vehicleId를 실어 보낸다.
        assertThat(failed.vehicleId()).isEqualTo("V-3");
        assertThat(failed.reason()).isEqualTo(PaymentFailedEvent.Reason.AUTHORIZATION_EXPIRED);
    }

    @Test
    void handleVehicleAssigned_throwsWhenAuthorizationMissing() {
        when(paymentRepository.findByReservationId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentService.handleVehicleAssigned(vehicleAssigned("missing", "V-1")))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void handleVehicleAssigned_throwsWhenPaymentNotAuthorized() {
        Payment payment = Payment.authorized("R-4", new BigDecimal("150000"));
        payment.voidAuthorization();
        when(paymentRepository.findByReservationId("R-4")).thenReturn(Optional.of(payment));

        assertThatThrownBy(() -> paymentService.handleVehicleAssigned(vehicleAssigned("R-4", "V-4")))
                .isInstanceOf(IllegalStateException.class);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void handleVehicleAssignFailed_voidsAuthorizationAndPublishesPaymentVoided() throws Exception {
        Payment payment = Payment.authorized("R-5", new BigDecimal("300000"));
        when(paymentRepository.findByReservationId("R-5")).thenReturn(Optional.of(payment));

        VehicleAssignFailedEvent event = new VehicleAssignFailedEvent(
                UUID.randomUUID(), Instant.now(), "R-5", 1, "R-5", VehicleAssignFailedEvent.Reason.OUT_OF_STOCK);

        paymentService.handleVehicleAssignFailed(event);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.VOIDED);

        OutboxEvent outboxEvent = capturedOutboxEvent();
        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentVoidedEvent.TYPE);

        PaymentVoidedEvent voided = objectMapper.readValue(outboxEvent.getPayload(), PaymentVoidedEvent.class);
        assertThat(voided.reservationId()).isEqualTo("R-5");
        assertThat(voided.paymentId()).isEqualTo(payment.getId());
    }
}
