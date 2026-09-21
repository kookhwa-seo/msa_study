package com.msastudy.vehicle.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.msastudy.common.event.PaymentAuthorizedEvent;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.domain.VehicleStatus;
import com.msastudy.vehicle.outbox.OutboxEvent;
import com.msastudy.vehicle.outbox.OutboxEventRepository;
import com.msastudy.vehicle.repository.ProcessedEventRepository;
import com.msastudy.vehicle.repository.VehicleRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class VehicleAssignmentServiceTest {

    private VehicleRepository vehicleRepository;
    private OutboxEventRepository outboxEventRepository;
    private ProcessedEventRepository processedEventRepository;
    private ObjectMapper objectMapper;
    private VehicleAssignmentService vehicleAssignmentService;

    @BeforeEach
    void setUp() {
        vehicleRepository = mock(VehicleRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        processedEventRepository = mock(ProcessedEventRepository.class);
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        vehicleAssignmentService = new VehicleAssignmentService(
                vehicleRepository, outboxEventRepository, processedEventRepository, objectMapper);
    }

    @Test
    void handlePaymentAuthorized_assignsAvailableVehicleAndPublishesAssigned() throws Exception {
        Vehicle vehicle = Vehicle.of("V-1", VehicleType.COMPACT, "SEOUL_GANGNAM");
        when(vehicleRepository.findFirstByVehicleTypeAndBranchIdAndStatus(
                VehicleType.COMPACT, "SEOUL_GANGNAM", VehicleStatus.AVAILABLE))
                .thenReturn(Optional.of(vehicle));

        PaymentAuthorizedEvent event = new PaymentAuthorizedEvent(
                UUID.randomUUID(), Instant.now(), "R-1", 1,
                "R-1", "PAY-1", new BigDecimal("150000"), VehicleType.COMPACT, "SEOUL_GANGNAM");

        vehicleAssignmentService.handlePaymentAuthorized(event);

        assertThat(vehicle.getStatus()).isEqualTo(VehicleStatus.ASSIGNED);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.VEHICLE_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(VehicleAssignedEvent.TYPE);
        assertThat(outboxEvent.getAggregateId()).isEqualTo("R-1");

        VehicleAssignedEvent assigned = objectMapper.readValue(outboxEvent.getPayload(), VehicleAssignedEvent.class);
        assertThat(assigned.vehicleId()).isEqualTo("V-1");
        assertThat(assigned.reservationId()).isEqualTo("R-1");
        assertThat(assigned.branchId()).isEqualTo("SEOUL_GANGNAM");
    }

    @Test
    void handlePaymentAuthorized_publishesAssignFailedWhenOutOfStock() throws Exception {
        when(vehicleRepository.findFirstByVehicleTypeAndBranchIdAndStatus(
                VehicleType.VAN, "BUSAN_HAEUNDAE", VehicleStatus.AVAILABLE))
                .thenReturn(Optional.empty());

        PaymentAuthorizedEvent event = new PaymentAuthorizedEvent(
                UUID.randomUUID(), Instant.now(), "R-2", 1,
                "R-2", "PAY-2", new BigDecimal("300000"), VehicleType.VAN, "BUSAN_HAEUNDAE");

        vehicleAssignmentService.handlePaymentAuthorized(event);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.VEHICLE_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(VehicleAssignFailedEvent.TYPE);

        VehicleAssignFailedEvent failed =
                objectMapper.readValue(outboxEvent.getPayload(), VehicleAssignFailedEvent.class);
        assertThat(failed.reason()).isEqualTo(VehicleAssignFailedEvent.Reason.OUT_OF_STOCK);
        assertThat(failed.reservationId()).isEqualTo("R-2");
    }

    @Test
    void handlePaymentFailed_releasesVehicleAndPublishesReleased() throws Exception {
        Vehicle vehicle = Vehicle.of("V-1", VehicleType.SUV, "SEOUL_GANGNAM");
        vehicle.assign();
        when(vehicleRepository.findById("V-1")).thenReturn(Optional.of(vehicle));

        PaymentFailedEvent event = new PaymentFailedEvent(
                UUID.randomUUID(), Instant.now(), "R-3", 1,
                "R-3", "V-1", PaymentFailedEvent.Reason.AUTHORIZATION_EXPIRED);

        vehicleAssignmentService.handlePaymentFailed(event);

        assertThat(vehicle.getStatus()).isEqualTo(VehicleStatus.AVAILABLE);

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(captor.capture());
        OutboxEvent outboxEvent = captor.getValue();

        assertThat(outboxEvent.getTopic()).isEqualTo(Topics.VEHICLE_EVENTS);
        assertThat(outboxEvent.getEventType()).isEqualTo(VehicleReleasedEvent.TYPE);

        VehicleReleasedEvent released = objectMapper.readValue(outboxEvent.getPayload(), VehicleReleasedEvent.class);
        assertThat(released.reservationId()).isEqualTo("R-3");
        assertThat(released.vehicleId()).isEqualTo("V-1");
    }

    @Test
    void handlePaymentFailed_throwsWhenVehicleNotFound() {
        when(vehicleRepository.findById("missing")).thenReturn(Optional.empty());

        PaymentFailedEvent event = new PaymentFailedEvent(
                UUID.randomUUID(), Instant.now(), "R-4", 1,
                "R-4", "missing", PaymentFailedEvent.Reason.CARD_DECLINED);

        assertThatThrownBy(() -> vehicleAssignmentService.handlePaymentFailed(event))
                .isInstanceOf(NoSuchElementException.class);
    }
}
