package com.msastudy.reservation.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.ReservationCancelledEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.reservation.domain.Reservation;
import com.msastudy.reservation.domain.ReservationStatus;
import com.msastudy.reservation.outbox.OutboxEvent;
import com.msastudy.reservation.outbox.OutboxEventRepository;
import com.msastudy.reservation.repository.ProcessedEventRepository;
import com.msastudy.reservation.repository.ReservationRepository;
import com.msastudy.reservation.web.CreateReservationRequest;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ReservationServiceTest {

    private ReservationRepository reservationRepository;
    private OutboxEventRepository outboxEventRepository;
    private ProcessedEventRepository processedEventRepository;
    private ObjectMapper objectMapper;
    private ReservationService reservationService;

    @BeforeEach
    void setUp() {
        reservationRepository = mock(ReservationRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        processedEventRepository = mock(ProcessedEventRepository.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        reservationService = new ReservationService(
                reservationRepository, outboxEventRepository, processedEventRepository, objectMapper);
    }

    @Test
    void createReservation_savesReservationAndOutboxEventInSameCall() throws Exception {
        CreateReservationRequest request = new CreateReservationRequest(
                "CUST-1",
                VehicleType.COMPACT,
                "SEOUL_GANGNAM",
                Instant.parse("2026-07-10T00:00:00Z"),
                Instant.parse("2026-07-12T00:00:00Z"),
                new BigDecimal("150000")
        );

        Reservation reservation = reservationService.createReservation(request);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.PENDING);
        verify(reservationRepository).save(reservation);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.RESERVATION_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(ReservationCreatedEvent.TYPE);
        assertThat(outboxEvent.getAggregateId()).isEqualTo(reservation.getId());
        assertThat(outboxEvent.getPublishedAt()).isNull();

        ReservationCreatedEvent event = objectMapper.readValue(outboxEvent.getPayload(), ReservationCreatedEvent.class);
        assertThat(event.eventType()).isEqualTo(ReservationCreatedEvent.TYPE);
        assertThat(event.reservationId()).isEqualTo(reservation.getId());
        assertThat(event.customerId()).isEqualTo("CUST-1");
        assertThat(event.vehicleType()).isEqualTo(VehicleType.COMPACT);
        assertThat(event.branchId()).isEqualTo("SEOUL_GANGNAM");
    }

    @Test
    void handleVehicleAssigned_marksPaymentPending() {
        Reservation reservation = Reservation.create(
                "CUST-1", VehicleType.COMPACT, "SEOUL_GANGNAM",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal("100000"));
        when(reservationRepository.findById(reservation.getId())).thenReturn(Optional.of(reservation));

        VehicleAssignedEvent event = new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), reservation.getId(), 1,
                reservation.getId(), "V-1", "SEOUL_GANGNAM");

        reservationService.handleVehicleAssigned(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.PAYMENT_PENDING);
    }

    @Test
    void handlePaymentCompleted_confirmsReservation() {
        Reservation reservation = Reservation.create(
                "CUST-1", VehicleType.COMPACT, "SEOUL_GANGNAM",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal("100000"));
        reservation.markPaymentPending();
        when(reservationRepository.findById(reservation.getId())).thenReturn(Optional.of(reservation));

        PaymentCompletedEvent event = new PaymentCompletedEvent(
                UUID.randomUUID(), Instant.now(), reservation.getId(), 1,
                reservation.getId(), "PAY-1", new BigDecimal("100000"), Instant.now());

        reservationService.handlePaymentCompleted(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CONFIRMED);
    }

    @Test
    void handleVehicleReleased_cancelsReservationAndPublishesCancelledEventWithPaymentFailedReason() throws Exception {
        Reservation reservation = Reservation.create(
                "CUST-1", VehicleType.SUV, "SEOUL_GANGNAM",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal("2000000"));
        reservation.markPaymentPending();
        when(reservationRepository.findById(reservation.getId())).thenReturn(Optional.of(reservation));

        VehicleReleasedEvent event = new VehicleReleasedEvent(
                UUID.randomUUID(), Instant.now(), reservation.getId(), 1,
                reservation.getId(), "V-1");

        reservationService.handleVehicleReleased(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CANCELLED);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getEventType()).isEqualTo(ReservationCancelledEvent.TYPE);
        ReservationCancelledEvent cancelled =
                objectMapper.readValue(outboxEvent.getPayload(), ReservationCancelledEvent.class);
        assertThat(cancelled.reason()).isEqualTo(ReservationCancelledEvent.Reason.PAYMENT_FAILED);
    }

    @Test
    void handleVehicleAssigned_throwsWhenReservationNotFound() {
        when(reservationRepository.findById("missing")).thenReturn(Optional.empty());
        VehicleAssignedEvent event = new VehicleAssignedEvent(
                UUID.randomUUID(), Instant.now(), "missing", 1, "missing", "V-1", "SEOUL_GANGNAM");

        assertThatThrownBy(() -> reservationService.handleVehicleAssigned(event))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void handleVehicleAssignFailed_cancelsReservationAndPublishesCancelledEvent() throws Exception {
        Reservation reservation = Reservation.create(
                "CUST-1", VehicleType.VAN, "BUSAN_HAEUNDAE",
                Instant.now(), Instant.now().plusSeconds(3600), new BigDecimal("300000"));
        when(reservationRepository.findById(reservation.getId())).thenReturn(Optional.of(reservation));

        VehicleAssignFailedEvent event = new VehicleAssignFailedEvent(
                UUID.randomUUID(), Instant.now(), reservation.getId(), 1,
                reservation.getId(), VehicleAssignFailedEvent.Reason.OUT_OF_STOCK);

        reservationService.handleVehicleAssignFailed(event);

        assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.CANCELLED);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.RESERVATION_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(ReservationCancelledEvent.TYPE);

        ReservationCancelledEvent cancelled =
                objectMapper.readValue(outboxEvent.getPayload(), ReservationCancelledEvent.class);
        assertThat(cancelled.reason()).isEqualTo(ReservationCancelledEvent.Reason.OUT_OF_STOCK);
        assertThat(cancelled.reservationId()).isEqualTo(reservation.getId());
    }
}
