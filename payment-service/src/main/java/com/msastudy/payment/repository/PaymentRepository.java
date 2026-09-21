package com.msastudy.payment.repository;

import com.msastudy.payment.domain.Payment;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, String> {

    Optional<Payment> findByReservationId(String reservationId);

    boolean existsByReservationId(String reservationId);
}
