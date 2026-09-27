package com.msastudy.vehicle.repository;

import com.msastudy.common.event.VehicleType;
import com.msastudy.vehicle.domain.Vehicle;
import com.msastudy.vehicle.domain.VehicleStatus;
import jakarta.persistence.LockModeType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface VehicleRepository extends JpaRepository<Vehicle, String> {

    /**
     * 락 없는 조회. 동시 배정 경합(같은 차종+지점에 대한 요청이 동시에 여러 건 들어오는 상황)에는
     * 이 메서드 혼자로는 안전하지 않다 - "재고 확인"과 "배정 표시" 사이에 다른 트랜잭션이 끼어들
     * 수 있다. 경합이 없는 조회(재고 수량 표시 등)나, 의도적으로 락 없는 경우를 비교하는
     * 실습에서만 쓴다({@code docs/concurrent-booking-lab.md} 참고).
     */
    Optional<Vehicle> findFirstByVehicleTypeAndBranchIdAndStatus(
            VehicleType vehicleType, String branchId, VehicleStatus status);

    long countByVehicleTypeAndBranchIdAndStatus(VehicleType vehicleType, String branchId, VehicleStatus status);

    /**
     * 비관적 락(SELECT ... FOR UPDATE)으로 딱 1행만 잠근다. 같은 조건에 여러 트랜잭션이 동시에
     * 들어오면, 먼저 도착한 트랜잭션이 커밋(또는 롤백)할 때까지 나머지는 이 조회 자체에서
     * 대기한다 - 그래서 같은 차량이 두 예약에 동시에 배정되는 경합이 원천적으로 불가능하다.
     * {@code Pageable}로 LIMIT 1을 걸어야 조건에 맞는 차량 전부가 아니라 딱 한 행만 잠근다.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT v FROM Vehicle v WHERE v.vehicleType = :vehicleType AND v.branchId = :branchId AND v.status = :status")
    List<Vehicle> lockFirstAvailable(
            @Param("vehicleType") VehicleType vehicleType,
            @Param("branchId") String branchId,
            @Param("status") VehicleStatus status,
            Pageable pageable);

    /**
     * 동시 배정 방어 실습(락 없음 비교군) 전용. {@code Vehicle}에는 {@code @Version}이 붙어
     * 있어서, 엔티티를 읽고 필드를 바꾼 뒤 저장하는 평범한 방식(findFirstBy... + assign() +
     * 더티체킹 flush)으로는 Hibernate가 자동으로 버전 체크를 끼워 넣어버려 "락이 전혀 없는"
     * 상태를 재현할 수 없다. 이 벌크 JPQL UPDATE는 영속성 컨텍스트를 거치지 않고 SQL로 바로
     * 나가서 버전 체크 자체가 없다 - 원래(락 도입 전) 코드의 동작을 정확히 재현하기 위해서만
     * 쓴다. 프로덕션에서는 사용하지 않는다.
     */
    @Modifying
    @Query("UPDATE Vehicle v SET v.status = :status WHERE v.id = :id")
    int updateStatusIgnoringVersion(@Param("id") String id, @Param("status") VehicleStatus status);
}
