package com.msastudy.vehicle.domain;

import com.msastudy.common.event.VehicleType;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "vehicle")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Vehicle {

    @Id
    private String id;

    @Enumerated(EnumType.STRING)
    private VehicleType vehicleType;

    private String branchId;

    @Enumerated(EnumType.STRING)
    private VehicleStatus status;

    /**
     * 낙관적 락 비교 실습(동시 배정 랩)을 위해 추가했다. {@code @Version}은 엔티티 매핑
     * 레벨의 기능이라, 비관적 락 경로(엔티티를 통해 {@code assign()} 호출 후 평범하게
     * flush)에서도 매 UPDATE마다 자동으로 버전 체크가 같이 걸린다 - "비관적 락은 이 필드와
     * 무관하다"고 착각하기 쉬운데 실제로는 아니다.
     *
     * <p><b>기존 데이터가 있는 테이블에 이 필드를 추가할 때 겪은 문제</b>: {@code ddl-auto:
     * update}는 컬럼만 추가할 뿐 기존 행의 값을 채워주지 않는다. 이 프로젝트는 이미
     * `VehicleInventorySeeder`로 6,875대를 시딩해둔 상태였는데, 이 필드를 추가한 뒤
     * 그 기존 행들은 전부 {@code version = NULL}이 됐다. Hibernate가 그런 행을 UPDATE하려고
     * 버전을 증가시키다가 {@code NullPointerException}(″Cannot invoke Long.longValue()
     * because current is null″)을 던졌다 - 새로 만든 차량(버전이 0으로 정상 초기화됨)은
     * 문제없어서 한동안 원인을 못 찾았다. {@code UPDATE vehicle SET version = 0 WHERE
     * version IS NULL;}로 기존 행을 백필하고 나서야 해결됐다 (`docs/concurrent-booking-lab.md`
     * 참고). Flyway 같은 마이그레이션 도구 없이 {@code ddl-auto: update}만 쓰는 이 프로젝트
     * 구조에서, 기존 데이터가 있는 테이블에 새 컬럼(특히 NOT NULL이 필요한 컬럼)을 추가할 때는
     * 항상 백필이 필요하다는 걸 몸으로 겪었다.
     */
    @Version
    private Long version;

    public static Vehicle of(String id, VehicleType vehicleType, String branchId) {
        Vehicle vehicle = new Vehicle();
        vehicle.id = id;
        vehicle.vehicleType = vehicleType;
        vehicle.branchId = branchId;
        vehicle.status = VehicleStatus.AVAILABLE;
        return vehicle;
    }

    public void assign() {
        this.status = VehicleStatus.ASSIGNED;
    }

    public void release() {
        this.status = VehicleStatus.AVAILABLE;
    }
}
