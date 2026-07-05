package com.msastudy.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.payment.domain.ReservationSnapshot;
import com.msastudy.payment.outbox.OutboxEvent;
import com.msastudy.payment.outbox.OutboxEventRepository;
import com.msastudy.payment.repository.PaymentRepository;
import com.msastudy.payment.repository.ReservationSnapshotRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class PaymentServiceTest {

    private static final BigDecimal FAIL_ABOVE_AMOUNT = new BigDecimal("1000000");

    private ReservationSnapshotRepository reservationSnapshotRepository;
    private PaymentRepository paymentRepository;
    private OutboxEventRepository outboxEventRepository;
    private ObjectMapper objectMapper;
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        reservationSnapshotRepository = mock(ReservationSnapshotRepository.class);
        paymentRepository = mock(PaymentRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        paymentService = new PaymentService(
                reservationSnapshotRepository, paymentRepository, outboxEventRepository, objectMapper, FAIL_ABOVE_AMOUNT);
    }

    @Test
    void handleReservationCreated_savesSnapshot() {
        ReservationCreatedEvent event = new ReservationCreatedEvent(
                UUID.randomUUID(), Instant.now(), "R-1", 1,
                "R-1", "CUST-1", VehicleType.COMPACT, "SEOUL_GANGNAM",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal("150000"));

        paymentService.handleReservationCreated(event);

        ArgumentCaptor<ReservationSnapshot> captor = ArgumentCaptor.forClass(ReservationSnapshot.class);
        verify(reservationSnapshotRepository).save(captor.capture());
        ReservationSnapshot snapshot = captor.getValue();

        assertThat(snapshot.getReservationId()).isEqualTo("R-1");
        assertThat(snapshot.getCustomerId()).isEqualTo("CUST-1");
        assertThat(snapshot.getTotalAmount()).isEqualByComparingTo("150000");
    }

    @Test
    void handleVehicleAssigned_publishesPaymentCompletedWhenAmountWithinLimit() throws Exception {
        ReservationSnapshot snapshot = ReservationSnapshot.of(
                "R-1", "CUST-1", VehicleType.COMPACT, new BigDecimal("150000"));
        when(reservationSnapshotRepository.findById("R-1")).thenReturn(Optional.of(snapshot));

        VehicleAssignedEvent event = new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), "R-1", 1, "R-1", "V-1", "SEOUL_GANGNAM");

        paymentService.handleVehicleAssigned(event);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.PAYMENT_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentCompletedEvent.TYPE);

        PaymentCompletedEvent completed = objectMapper.readValue(outboxEvent.getPayload(), PaymentCompletedEvent.class);
        assertThat(completed.reservationId()).isEqualTo("R-1");
        assertThat(completed.amount()).isEqualByComparingTo("150000");
    }

    @Test
    void handleVehicleAssigned_publishesPaymentFailedWhenAmountExceedsLimit() throws Exception {
        ReservationSnapshot snapshot = ReservationSnapshot.of(
                "R-2", "CUST-2", VehicleType.SUV, new BigDecimal("2000000"));
        when(reservationSnapshotRepository.findById("R-2")).thenReturn(Optional.of(snapshot));

        VehicleAssignedEvent event = new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), "R-2", 1, "R-2", "V-2", "SEOUL_GANGNAM");

        paymentService.handleVehicleAssigned(event);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getEventType()).isEqualTo(PaymentFailedEvent.TYPE);

        PaymentFailedEvent failed = objectMapper.readValue(outboxEvent.getPayload(), PaymentFailedEvent.class);
        assertThat(failed.reservationId()).isEqualTo("R-2");
        assertThat(failed.vehicleId()).isEqualTo("V-2");
        assertThat(failed.reason()).isEqualTo(PaymentFailedEvent.Reason.INSUFFICIENT_LIMIT);
    }

    @Test
    void handleVehicleAssigned_throwsWhenSnapshotMissing() {
        when(reservationSnapshotRepository.findById("missing")).thenReturn(Optional.empty());

        VehicleAssignedEvent event = new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), "missing", 1, "missing", "V-1", "SEOUL_GANGNAM");

        assertThatThrownBy(() -> paymentService.handleVehicleAssigned(event))
                .isInstanceOf(NoSuchElementException.class);
    }
}
