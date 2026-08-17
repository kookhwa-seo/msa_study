package com.msastudy.reservation.repository;

import com.msastudy.reservation.domain.Reservation;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationRepository extends JpaRepository<Reservation, String> {

    List<Reservation> findAllByOrderByCreatedAtDesc();

    List<Reservation> findAllByCustomerIdOrderByCreatedAtDesc(String customerId);
}
