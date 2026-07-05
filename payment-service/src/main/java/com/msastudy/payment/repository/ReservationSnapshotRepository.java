package com.msastudy.payment.repository;

import com.msastudy.payment.domain.ReservationSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ReservationSnapshotRepository extends JpaRepository<ReservationSnapshot, String> {
}
