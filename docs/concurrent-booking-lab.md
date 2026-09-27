# 동시 예약(차량 배정) 방어 실습

## 요청받은 내용과 실제 도메인의 차이

원래 요청은 "`reservation` 테이블에 `(vehicle_id, time_slot)` 복합 유니크 제약을 걸어서 동시
예약을 막는다"였다. 그런데 이 프로젝트의 실제 도메인은 그 전제와 안 맞는다:

- `reservation-service`의 `Reservation`에는 `vehicleId`가 없다 — `vehicleType`(차종 카테고리)과
  `branchId`만 있고, 구체적인 차량은 vehicle-service가 나중에 비동기로 배정한다.
- `Vehicle`(vehicle-service)에는 `timeSlot` 개념 자체가 없다 — 차량은 그냥 `AVAILABLE`/
  `ASSIGNED` 상태만 있는 단순 재고 모델이라, 대여 기간과 무관하게 배정되면 그걸로 끝이다.

그래서 실제로 존재하는, 요청 의도와 가장 가까운 동시성 버그를 코드에서 찾아서 그걸 고쳤다:
`VehicleAssignmentService.handlePaymentAuthorized`(당시 이름)가 "AVAILABLE 차량 조회" →
"assign() 표시"를 락 없이 했다 — 같은 차종+지점에 대한 가승인이 동시에 여러 건 들어오면
**같은 물리적 차량이 서로 다른 두 예약에 동시에 배정될 수 있는** 진짜 레이스 컨디션이었다.
이건 "유니크 제약으로 막는 중복 INSERT" 문제(이미 끝낸 2번 항목, 결제 UK)가 아니라 "기존 행 하나를
두 트랜잭션이 동시에 집으려는" 문제라서, 정답이 유니크 제약이 아니라 **락**(비관적 또는 낙관적)
이다.

## 구현

`vehicle-service`의 `VehicleAssignmentService`에 같은 문제를 푸는 세 가지 방식을 나란히 뒀다.
Kafka 리스너(`PaymentEventConsumer`)가 실제로 부르는 건 `handlePaymentAuthorized`
(비관적 락) 하나뿐이고, 나머지 둘은 비교/재현 전용이다.

1. **`handlePaymentAuthorized` (프로덕션, 비관적 락)** — `VehicleRepository.lockFirstAvailable`이
   `@Lock(LockModeType.PESSIMISTIC_WRITE)` + `Pageable(0,1)`로 조건에 맞는 차량 딱 1행을
   `SELECT ... FOR UPDATE LIMIT 1`로 잠근다. 동시에 들어온 다른 요청은 이 조회 자체에서
   대기하므로 경합이 원천적으로 불가능하다.
2. **`handlePaymentAuthorizedOptimistic` (비교 전용)** — `Vehicle`에 추가한 `@Version` 필드를
   이용한다. 미리 잠그지 않고 각자 조회 후 `assign()`하는데, 저장 시점에 버전이 이미 바뀌어
   있으면 `ObjectOptimisticLockingFailureException`이 터진다. `TransactionTemplate`으로
   트랜잭션 경계를 좁혀서 이 예외를 롤백 이후 바깥에서 처리한다(결제 UK 처리와 같은 이유).
3. **`handlePaymentAuthorizedWithoutLock` (버그 재현용)** — 원래(락 도입 전) 코드 그대로.
   `Vehicle`에 `@Version`이 있어서 엔티티를 통한 평범한 저장은 자동으로 버전 체크가 걸려버려
   "락이 전혀 없는" 상태를 재현할 수 없다 — 그래서 이 메서드만 영속성 컨텍스트를 거치지 않는
   벌크 JPQL UPDATE(`updateStatusIgnoringVersion`)를 직접 써서 진짜로 아무 보호도 없게 만들었다.

## 검증 1: 동시성 유닛 테스트 (`VehicleAssignmentConcurrencyTest`)

차량 1대만 있는 상태에서 같은 차종+지점을 놓고 경쟁하는 `PaymentAuthorized` 20건을
`CountDownLatch`로 동시에 발행하고, 실제로 `VehicleAssigned`가 몇 건 나오는지(=몇 건이 그
차량을 "받았다"고 응답받았는지) 센다. 실제 로컬 vehicle-db가 필요해서 `@Tag("integration")`으로
표시했고 기본 `./gradlew test`에서는 빠진다.

```bash
docker compose up -d vehicle-db
./gradlew :vehicle-service:concurrencyTest
```

### 실제 실행 결과

```
[without-lock]      20건 동시 요청 중 VehicleAssigned 발행 건수: 20  (전부 같은 vehicleId로 중복 배정!)
[optimistic-lock]   20건 동시 요청 중 VehicleAssigned 발행 건수: 1
[pessimistic-lock]  20건 동시 요청 중 VehicleAssigned 발행 건수: 1
```

**"동시 20건 요청 시 락 미적용 상태에서 중복 배정 19건 발생(20건 전부 같은 차량에 배정)
→ 락(비관적/낙관적 모두) 적용 후 0건"**을 그대로 확인했다.

락 방식별 20건 처리 시간(같은 실행에서 측정, 참고용):

```
without-lock(인위적 30ms 지연 포함) : 105ms
optimistic-lock                    :  29ms
pessimistic-lock                   :  57ms
```

`without-lock`은 경합을 눈에 보이게 만들려고 일부러 30ms sleep을 넣은 버전이라 다른 둘과
시간을 직접 비교할 수 없다(자세한 이유는 아래 "왜 without-lock에 인위적 지연을 넣었나" 참고).
**의미 있는 비교는 optimistic 대 pessimistic**이다: 비관적 락은 진 쪽이 이긴 쪽의 커밋을
"기다리기" 때문에 느리고, 낙관적 락은 아무도 기다리지 않고 각자 진행하다가 진 쪽만 "실패"로
빠르게 끝나기 때문에 더 빠르다 — 경합이 심할수록 이 차이는 더 벌어질 수 있다.

### 왜 without-lock에 인위적 지연을 넣었나

로컬 Postgres에서는 조회~커밋이 1ms도 안 걸려서, 아무 보호 장치가 없어도 20개 스레드를
동시에 던졌을 때 우연히 안 겹칠 때가 있었다(처음 실행했을 때 실제로 그랬다 - 중복 0건).
"락이 없으면 항상 안전하지 않다"는 걸 신뢰성 있게 보여주려고, 조회와 저장 사이에 30ms
sleep을 일부러 넣어 경합 창을 넓혔다. 이 지연은 실제 재고 차감 로직이 그만큼 걸린다는 뜻이
아니라 순전히 실습용 장치다.

## 검증 2: 실제 HTTP 부하 테스트로 전체 파이프라인 확인 (k6)

유닛 테스트는 `VehicleAssignmentService`만 직접 부르지만, k6로는 진짜 `POST
/api/reservations` → Kafka(reservation-events → payment-events) → vehicle-service까지
전체 Choreography Saga를 실제로 통과시켜 같은 결론을 다시 확인했다.

### 정합성 검증 스크립트 (`infra/k6/concurrent-booking-correctness.js`)

같은 차종+지점에 동시에 20명이 예약을 시도하고, 각자 자기 예약이 최종적으로
`PAYMENT_PENDING`/`CONFIRMED`(차량을 받음)인지 `CANCELLED`(재고 부족으로 거절)인지 폴링해서
센다. 실행 전에 대상 (COMPACT, `1111010100`) 조합의 실제 AVAILABLE 재고를 정확히 1대로
맞춰뒀다.

```bash
BASE_URL=http://localhost:8081 BRANCH_ID=1111010100 VU_COUNT=20 \
  k6 run infra/k6/concurrent-booking-correctness.js
```

**실제 실행 결과 (2회 반복, 재현됨):**

```
1차: reservation_got_vehicle=1, reservation_rejected_out_of_stock=19  (3.1초 만에 전부 정리)
2차: reservation_got_vehicle=1, reservation_rejected_out_of_stock=19  (6.3초 만에 전부 정리)
```

재고가 1대뿐인 상황에 20명이 동시에 몰려도 **정확히 1명만 차량을 받고 나머지 19명은 정상적으로
거절됐다** — 실제 HTTP 요청 + Kafka + 방금 고친 비관적 락까지 전부 거친 end-to-end 증거다.

### 성능 측정 스크립트 (`infra/k6/reservation-ramp-up.js`)

`POST /api/reservations` 자체(reservation-service의 동기 write 경로)를 10 → 50 → 100 → 200명까지
단계적으로 올리며 측정했다.

```bash
BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-ramp-up.js
```

**실제 실행 결과:**

```
총 요청 445,076건, 에러율 0.00%
TPS: 3,296.8 req/s
p90: 47.32ms  p95: 57.56ms  p99: 93.65ms
```

**중요한 점**: 이번에 추가한 락(비관적 락)은 `POST /api/reservations`가 아니라
vehicle-service의 **비동기 Kafka 컨슈머**(`handlePaymentAuthorized`) 안에 있다. 그래서 이
API 자체의 TPS/레이턴시는 락 적용 전후로 사실상 차이가 없다 — 락의 성능 비용은 위
유닛 테스트가 잰 "20건 처리 시간"(pessimistic 57ms vs optimistic 29ms) 쪽에서 봐야 한다.
"부하 테스트로 TPS/레이턴시가 왜 안 변했는지"까지가 이 실습에서 확인하려던 것 중 하나다 —
락을 어디에 걸었는지에 따라 그 비용이 어느 지표에 나타나는지가 달라진다.

## 실습 중 겪은 진짜 버그: `@Version`을 기존 데이터가 있는 테이블에 추가할 때

낙관적 락 비교를 위해 `Vehicle`에 `@Version` 필드를 추가했는데, k6로 실제 파이프라인을 태워보니
계속 타임아웃이 났다. 원인을 추적해보니:

- 이 프로젝트는 `ddl-auto: update`만 쓰고 Flyway 같은 마이그레이션 도구가 없다.
- `ddl-auto: update`는 새 컬럼을 추가만 할 뿐, **기존 행의 값을 채워주지 않는다.**
- `VehicleInventorySeeder`가 이미 6,875대를 시딩해둔 상태에서 `@Version`을 추가했더니, 그
  기존 행들은 전부 `version = NULL`이 됐다.
- Hibernate가 그런 행을 UPDATE하려고 버전을 증가시키다가
  `NullPointerException: Cannot invoke "Long.longValue()" because "current" is null`을 던졌다.
- 이 테스트가 직접 만드는 차량(`V-CONC-...`)은 추가 후 새로 INSERT되어 버전이 정상(0)으로
  초기화돼서 문제가 없었다 — 그래서 유닛 테스트는 계속 통과하는데 실제 시딩된 데이터로
  붙인 k6 E2E 테스트만 계속 실패하는, 원인을 헷갈리기 딱 좋은 조합이었다.
- `UPDATE vehicle SET version = 0 WHERE version IS NULL;`로 기존 행을 백필하고 나서야
  해결됐다.

일반화하면: **마이그레이션 도구 없이 `ddl-auto: update`만 쓰는 프로젝트에서, 기존 데이터가
있는 테이블에 새 컬럼(특히 `@Version`처럼 NULL이면 안 되는 컬럼)을 추가할 때는 항상 백필이
필요하다**는 걸 실제로 겪었다.

## 결론

- "동시 예약 방어"를 문자 그대로의 스펙(`vehicle_id + time_slot` 유니크 제약)이 아니라, 실제
  도메인에 존재하는 진짜 레이스 컨디션(같은 차량 중복 배정)에 맞게 재해석해서 고쳤다 — 이
  문제의 정답은 유니크 제약이 아니라 락이었다.
- 비관적 락과 낙관적 락 둘 다 정합성은 완벽하게 지킨다(중복 0건). 차이는 성능 특성이다:
  비관적 락은 진 쪽을 기다리게 해서 상대적으로 느리고, 낙관적 락은 진 쪽을 빠르게 실패시켜서
  더 빠르다 — 다만 낙관적 락은 실패를 애플리케이션이 직접 처리(재시도 또는 포기)해야 한다는
  책임이 따라온다.
- 락의 성능 비용은 "락을 건 지점"에서 나타난다. 이번 락은 비동기 컨슈머 안에 있어서 동기 API
  (`POST /api/reservations`)의 TPS/레이턴시에는 영향이 없었다 — 부하 테스트 결과를 해석할 때
  "이 지표가 실제로 무엇을 측정하고 있는지"를 먼저 확인해야 한다는 걸 보여주는 사례다.
