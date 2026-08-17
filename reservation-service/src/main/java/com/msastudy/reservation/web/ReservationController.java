package com.msastudy.reservation.web;

import com.msastudy.reservation.domain.Reservation;
import com.msastudy.reservation.repository.ReservationRepository;
import com.msastudy.reservation.service.ReservationService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/reservations")
@RequiredArgsConstructor
public class ReservationController {

    private final ReservationService reservationService;
    private final ReservationRepository reservationRepository;

    @PostMapping
    public ResponseEntity<ReservationResponse> create(@Valid @RequestBody CreateReservationRequest request) {
        Reservation reservation = reservationService.createReservation(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(ReservationResponse.from(reservation));
    }

    @GetMapping("/{reservationId}")
    public ReservationResponse get(@PathVariable String reservationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "존재하지 않는 예약: " + reservationId));
        return ReservationResponse.from(reservation);
    }

    @GetMapping
    public List<ReservationResponse> list(@RequestParam(required = false) String customerId) {
        List<Reservation> reservations = customerId == null
                ? reservationRepository.findAllByOrderByCreatedAtDesc()
                : reservationRepository.findAllByCustomerIdOrderByCreatedAtDesc(customerId);
        return reservations.stream().map(ReservationResponse::from).toList();
    }
}
