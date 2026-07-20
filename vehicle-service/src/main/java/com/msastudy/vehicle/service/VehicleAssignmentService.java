package com.msastudy.vehicle.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentFailedEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class VehicleAssignmentService {

    private static final int SCHEMA_VERSION = 1;

    private final VehicleRepository vehicleRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * 재고 배정 시도 + outbox 저장을 하나의 트랜잭션으로 묶는다. 배정 성공/실패 어느 쪽이든
     * 결과 이벤트는 outbox를 거쳐 나가므로, 배정 상태 변경과 이벤트 발행 사이에 이중 쓰기
     * 문제가 생기지 않는다.
     *
     * Kafka는 at-least-once 전달이라 같은 ReservationCreated가 재전달될 수 있다. eventId가
     * 이미 처리된 적 있으면 재배정을 시도하지 않고 건너뛴다 — 그렇지 않으면 재고가 남아있을 때
     * 같은 예약에 차량이 두 번 배정되고 VehicleAssigned도 중복 발행될 수 있다.
     */
    @Transactional
    public void handleReservationCreated(ReservationCreatedEvent event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("이미 처리한 이벤트, 스킵: eventId={}, type={}", event.eventId(), ReservationCreatedEvent.TYPE);
            return;
        }

        Optional<Vehicle> available = vehicleRepository.findFirstByVehicleTypeAndBranchIdAndStatus(
                event.vehicleType(), event.branchId(), VehicleStatus.AVAILABLE);

        if (available.isEmpty()) {
            publishAssignFailed(event);
        } else {
            Vehicle vehicle = available.get();
            vehicle.assign();
            publishAssigned(event, vehicle);
        }

        processedEventRepository.save(ProcessedEvent.of(event.eventId()));
    }

    /**
     * 결제 실패에 대한 보상 트랜잭션: 배정했던 차량을 다시 재고로 돌리고, 그 결과를
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

    private void publishAssigned(ReservationCreatedEvent event, Vehicle vehicle) {
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

    private void publishAssignFailed(ReservationCreatedEvent event) {
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
}
