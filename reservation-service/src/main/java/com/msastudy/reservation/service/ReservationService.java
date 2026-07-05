package com.msastudy.reservation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.PaymentCompletedEvent;
import com.msastudy.common.event.ReservationCancelledEvent;
import com.msastudy.common.event.ReservationCreatedEvent;
import com.msastudy.common.event.Topics;
import com.msastudy.common.event.VehicleAssignFailedEvent;
import com.msastudy.common.event.VehicleAssignedEvent;
import com.msastudy.common.event.VehicleReleasedEvent;
import com.msastudy.reservation.domain.Reservation;
import com.msastudy.reservation.outbox.OutboxEvent;
import com.msastudy.reservation.outbox.OutboxEventRepository;
import com.msastudy.reservation.repository.ReservationRepository;
import com.msastudy.reservation.web.CreateReservationRequest;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ReservationService {

    private static final int SCHEMA_VERSION = 1;

    private final ReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * 예약 저장 + outbox 저장을 하나의 트랜잭션으로 묶는다 (Outbox 패턴의 핵심).
     * Kafka로의 실제 발행은 여기서 하지 않고 OutboxPublisher가 별도로 폴링해서 처리한다.
     */
    @Transactional
    public Reservation createReservation(CreateReservationRequest request) {
        Reservation reservation = Reservation.create(
                request.customerId(),
                request.vehicleType(),
                request.branchId(),
                request.rentalStartAt(),
                request.rentalEndAt(),
                request.totalAmount()
        );
        reservationRepository.save(reservation);

        ReservationCreatedEvent event = new ReservationCreatedEvent(
                UUID.randomUUID(),
                Instant.now(),
                reservation.getId(),
                SCHEMA_VERSION,
                reservation.getId(),
                reservation.getCustomerId(),
                reservation.getVehicleType(),
                reservation.getBranchId(),
                reservation.getRentalStartAt(),
                reservation.getRentalEndAt(),
                reservation.getTotalAmount()
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.RESERVATION_EVENTS, reservation.getId(), ReservationCreatedEvent.TYPE, writeJson(event)));

        return reservation;
    }

    /**
     * 차량 배정은 됐지만 아직 결제가 끝나지 않은 상태. 실제 CONFIRMED 전환은
     * PaymentCompleted 이벤트를 받았을 때(handlePaymentCompleted) 이뤄진다.
     */
    @Transactional
    public void handleVehicleAssigned(VehicleAssignedEvent event) {
        Reservation reservation = getOrThrow(event.reservationId());
        reservation.markPaymentPending();
    }

    @Transactional
    public void handleVehicleAssignFailed(VehicleAssignFailedEvent event) {
        Reservation reservation = getOrThrow(event.reservationId());
        reservation.cancel();
        publishCancelled(reservation, ReservationCancelledEvent.Reason.OUT_OF_STOCK);
    }

    @Transactional
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        Reservation reservation = getOrThrow(event.reservationId());
        reservation.confirm();
    }

    /**
     * 차량 배정까지는 성공했지만 결제가 실패해 vehicle-service가 배정을 취소(보상)한
     * 뒤 보낸 이벤트. 이 시점에 비로소 예약을 최종 취소 처리한다 (Saga 보상의 마지막 단계).
     */
    @Transactional
    public void handleVehicleReleased(VehicleReleasedEvent event) {
        Reservation reservation = getOrThrow(event.reservationId());
        reservation.cancel();
        publishCancelled(reservation, ReservationCancelledEvent.Reason.PAYMENT_FAILED);
    }

    private void publishCancelled(Reservation reservation, ReservationCancelledEvent.Reason reason) {
        ReservationCancelledEvent cancelled = new ReservationCancelledEvent(
                UUID.randomUUID(),
                Instant.now(),
                reservation.getId(),
                SCHEMA_VERSION,
                reservation.getId(),
                reason
        );
        outboxEventRepository.save(OutboxEvent.of(
                Topics.RESERVATION_EVENTS, reservation.getId(), ReservationCancelledEvent.TYPE, writeJson(cancelled)));
    }

    private Reservation getOrThrow(String reservationId) {
        return reservationRepository.findById(reservationId)
                .orElseThrow(() -> new NoSuchElementException("존재하지 않는 예약: " + reservationId));
    }

    private String writeJson(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("이벤트 직렬화 실패: " + event, e);
        }
    }
}
