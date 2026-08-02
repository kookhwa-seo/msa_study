# MSA Rental Study

렌터카 예약(카밀라 도메인: 렌터카/보험대차/예약/결제)을 소재로 MSA 서비스 분리, Kafka 이벤트
기반 통신, Outbox 패턴, Saga(Choreography)를 익히기 위한 **학습용** 프로젝트입니다.

자세한 설계 배경/원칙은 [`CLAUDE.md`](./CLAUDE.md), 이벤트 스키마 전체 정의는
[`docs/event-schema.md`](./docs/event-schema.md)를 참고하세요.

## 아키텍처

```
[Reservation] --ReservationCreated--> [Vehicle] --VehicleAssigned--> [Payment] --PaymentCompleted--> [Reservation]
   (Outbox)         |                  (Outbox)         |              (Outbox)         |
                    |                                   |                               └─(알림)──> [Notification]
                    └--VehicleAssignFailed--> [Reservation: CANCELLED]                  ┌─(알림)──> [Notification]
                                                                        PaymentFailed────┘
                                                                              |
                                                            [Vehicle: 재고 반환(보상)] --VehicleReleased--> [Reservation: CANCELLED]
```

- **Database per Service**: 서비스별 Postgres 인스턴스 분리 (reservation-db / vehicle-db / payment-db).
  notification-service는 DB 없음.
- **Choreography Saga**: 중앙 오케스트레이터 없이 각 서비스가 이벤트를 구독해 자기 몫을 처리하고
  다음 이벤트를 발행. 결제 실패 보상은 "실패 이벤트에 직접 반응"이 아니라 "보상을 수행한 서비스가
  보상 완료 이벤트를 발행 → 그걸 받은 서비스가 최종 처리"하는 흐름으로 구현했습니다
  (`PaymentFailed` → vehicle-service가 재고 반환 → `VehicleReleased` → reservation-service가 최종 취소).
- **Outbox 패턴**: 비즈니스 데이터 저장 + 이벤트 저장을 하나의 DB 트랜잭션으로 묶고, 별도
  폴링 퍼블리셔(`@Scheduled`, 2초 주기)가 Kafka로 발행. Debezium 같은 CDC 도구 없이 직접
  구현했습니다 (학습 목적 — 왜 Outbox가 필요한지 코드로 확인).
- 각 서비스가 자기 `OutboxEvent` 엔티티/퍼블리셔를 독립적으로 소유합니다 (`common` 모듈에
  추상화하지 않음).
- payment-service는 reservation-service를 동기 호출하지 않고, `ReservationCreated`를 구독해
  결제에 필요한 금액 정보만 로컬에 복사해둡니다(`ReservationSnapshot`) — Choreography Saga에서
  "필요한 데이터는 이벤트로 전달받아 로컬에 보관" 패턴을 보여주는 지점입니다.

## 서비스 구성

| 서비스 | 포트 | 담당 | 상태 |
|---|---|---|---|
| reservation-service | 8081 | 예약 생성/조회, Saga 시작점 | 구현됨 |
| vehicle-service | 8082 | 차량 재고/배정, 결제 실패 보상(재고 반환) | 구현됨 |
| payment-service | 8083 | 결제 처리(금액 기준 성공/실패 시뮬레이션) | 구현됨 |
| notification-service | 8084 | 알림(로그 시뮬레이션) | 구현됨 |

예약 상태 전이: `PENDING` → (배정 성공) `PAYMENT_PENDING` → (결제 성공) `CONFIRMED`,
또는 `PENDING`/`PAYMENT_PENDING` → (배정 실패 또는 결제 실패 보상 완료) `CANCELLED`.
자세한 이벤트/상태 다이어그램은 [`docs/event-schema.md`](./docs/event-schema.md) 참고.

## 기술 스택

- Spring Boot 3.4, Java 21 (Gradle Kotlin DSL 멀티모듈)
- Kafka (KRaft 모드, Zookeeper 없음), Kafka UI
- PostgreSQL 16 (서비스별 분리)
- Docker Compose로 인프라 구동

## 로컬 실행

### 사전 준비

```bash
# Java 21 (sdkman)
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 21.0.5-tem
```

Gradle은 wrapper(`./gradlew`)를 사용하므로 별도 설치가 필요 없습니다.

### 1. 인프라 기동

```bash
docker compose up -d
```

- Kafka: `localhost:9092`
- Kafka UI: http://localhost:8080
- reservation-db: `localhost:5432` / vehicle-db: `localhost:5433` / payment-db: `localhost:5434`

### 2. 서비스 실행 (각각 별도 터미널 또는 IntelliJ 런 컨피그)

```bash
./gradlew :reservation-service:bootRun
./gradlew :vehicle-service:bootRun
./gradlew :payment-service:bootRun
./gradlew :notification-service:bootRun
```

기동 확인 (notification-service는 웹 서버가 없어 헬스체크 엔드포인트가 없습니다 — 로그로 확인):

```bash
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
```

vehicle-service는 최초 기동 시 샘플 재고(COMPACT x2, SUV x1, VAN x1, 지점 `SEOUL_GANGNAM`)를
자동으로 시딩합니다. payment-service는 `app.payment.fail-above-amount`(기본 1,000,000원)를
초과하는 예약을 결제 실패로 시뮬레이션합니다 (`payment-service/src/main/resources/application.yml`).

## API 사용 예시

### 1. 재고 있음 + 결제 성공 → 자동 확정

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "vehicleType": "SUV",
    "branchId": "SEOUL_GANGNAM",
    "rentalStartAt": "2026-07-10T00:00:00Z",
    "rentalEndAt": "2026-07-12T00:00:00Z",
    "totalAmount": 200000
  }'
```

응답의 `reservationId`로 상태를 조회하면 수 초 내(outbox 폴링 주기 2초 x 왕복 홉 수) `PENDING`
→ `PAYMENT_PENDING`(차량 배정 완료, 결제 대기) → `CONFIRMED`(결제 완료)로 바뀝니다.

```bash
curl http://localhost:8081/api/reservations/{reservationId}
```

### 2. 재고 없음 → 즉시 취소

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-2",
    "vehicleType": "VAN",
    "branchId": "BUSAN_HAEUNDAE",
    "rentalStartAt": "2026-07-10T00:00:00Z",
    "rentalEndAt": "2026-07-12T00:00:00Z",
    "totalAmount": 300000
  }'
```

vehicle-service가 재고를 찾지 못해 `VehicleAssignFailed`를 발행하고, reservation-service가
이를 소비해 `PENDING` → `CANCELLED`로 전환하며 `ReservationCancelled` 이벤트를 다시 발행합니다.

### 3. 재고 있음 + 결제 실패(금액 초과) → 배정 취소 보상 후 최종 취소

`totalAmount`가 `app.payment.fail-above-amount`(기본 1,000,000)를 넘으면 결제가 실패합니다.

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-3",
    "vehicleType": "VAN",
    "branchId": "SEOUL_GANGNAM",
    "rentalStartAt": "2026-08-01T00:00:00Z",
    "rentalEndAt": "2026-08-05T00:00:00Z",
    "totalAmount": 2000000
  }'
```

흐름: `PENDING` → `PAYMENT_PENDING`(배정 성공) → payment-service가 `PaymentFailed` 발행 →
vehicle-service가 배정했던 차량을 재고로 되돌리고 `VehicleReleased` 발행 → reservation-service가
`CANCELLED`로 최종 전환. 배정됐던 차량이 다시 `AVAILABLE`로 돌아온 것도 vehicle-db에서 확인할
수 있습니다.

## 테스트

```bash
./gradlew test
```

`ReservationServiceTest`, `VehicleAssignmentServiceTest`, `PaymentServiceTest`가 각 서비스의
핵심 로직(예약 생성/배정/결제 시도 시 Outbox 저장, 성공/실패에 따른 상태 전이와 Outbox 이벤트
내용, 결제 실패 시 차량 재고 반환)을 검증합니다.

## 진행 로드맵

- [x] 1. 프로젝트 구조 / 기술 스택 세팅
- [x] 2. 이벤트 스키마 설계
- [x] 3~4. Reservation ↔ Vehicle, Outbox 패턴 포함 Kafka 이벤트 통신
- [x] 5. Payment 서비스 추가 + Saga 체인 완성 (결제 실패 시 보상 트랜잭션)
- [ ] 6. (선택) Debezium CDC 전환, Kafka Streams 모니터링

## 학습 회고: 왜 공부했고 무엇을 배웠는지

실무에서는 렌터카 도메인을 멀티 모듈 형태(모놀리식)로 개발하고 있는데, 나중에 서비스 트래픽이
늘어나 MSA 전환이 필요해지는 시점이 오면 서비스를 어떻게 나누고 실제로 MSA로 구현해야 할지
미리 감을 잡아두고 싶어서 시작한 프로젝트다. 그래서 실무 도메인을 단순화한 렌터카 예약 도메인을
그대로 가져와서, Debezium 같은 CDC 도구로 손쉽게 처리할 수 있는 부분도 처음엔 일부러 직접
구현해보며(`@Scheduled` 폴링 퍼블리셔) "MSA로 나눴을 때 프레임워크가 대신 해결해주는 문제가
정확히 뭔지"를 체감하는 것을 원칙으로 삼았다. 아래는 단계별로 어떤 질문에서 출발했고 무엇을
확인했는지 정리한 것이다.

- **MSA 서비스 분리 (Database per Service)** — 왜: 서비스를 나눈다는 말은 알아도 "DB까지 왜
  분리해야 하는지", 동기 호출 없이 서비스가 필요한 데이터를 어떻게 확보하는지 감이 없었다.
  배운 것: payment-service가 reservation-service를 REST로 호출하는 대신 `ReservationCreated`를
  구독해서 결제에 필요한 최소한의 정보(`ReservationSnapshot`)만 로컬에 복제해두는 패턴을 직접
  구현해봄으로써, "서비스 간 결합을 없앤다"는 게 실제로 어떤 트레이드오프(데이터 중복 vs 독립성)인지
  이해했다.
- **Kafka 파티션 키와 순서 보장** — 왜: Kafka가 순서를 보장한다는 걸 개념으로만 알고 있었고,
  실제로 무엇을 key로 잡아야 하는지 감이 없었다. 배운 것: 같은 예약(Saga)에 속한 이벤트가
  전부 같은 파티션으로 가도록 메시지 key를 항상 `reservationId`로 고정했고, 이벤트 봉투
  (`eventId/eventType/occurredAt/aggregateId/version`)를 설계하며 스키마 버저닝 개념도 함께
  익혔다 (`docs/event-schema.md`).
- **Outbox 패턴** — 왜: "DB에 저장한 다음 바로 Kafka로 발행하면 왜 안 되는지"(dual write
  problem)를 라이브러리 뒤에 숨기지 않고 직접 겪어보고 싶었다. 배운 것: 비즈니스 데이터 저장과
  `OutboxEvent` 저장을 하나의 트랜잭션으로 묶고, 별도 폴링(2초 주기 `@Scheduled`)이 이를 읽어
  Kafka로 발행하는 구조를 직접 구현하면서, 발행 쪽 재시도가 얼마나 단순/불완전할 수 있는지도
  체감했다 (아래 알려진 한계 참고).
- **Choreography Saga와 보상 트랜잭션** — 왜: 중앙 오케스트레이터 없이 서비스들이 이벤트에만
  반응해서 전체 흐름이 어떻게 일관되게 끝나는지 궁금했다. 배운 것: 실패에 반응해야 하는 주체는
  "실패를 발생시킨 이벤트"가 아니라 "보상을 실제로 수행해야 하는 서비스"라는 원칙을 실제로
  적용해봤다 — `PaymentFailed`는 vehicle-service가 구독해 배정을 취소(보상)하고, 그 결과인
  `VehicleReleased`를 reservation-service가 구독해서 최종 취소한다. reservation-service가
  `PaymentFailed`를 직접 구독하지 않는 이유를 설계하면서 명확해졌다.
- **컨슈머 멱등성 (at-least-once 전달)** — 왜: Kafka가 최소 한 번 전달을 보장한다는 건 알았지만,
  중복 수신이 실제로 어떤 문제(중복 배정, 중복 결제)를 일으키는지, 어떻게 막는지 직접 보고 싶었다.
  배운 것: 서비스마다 `ProcessedEvent`(eventId 처리 원장)를 두고, 이미 처리한 `eventId`면 비즈니스
  로직을 다시 실행하지 않도록 구현. 동일 메시지를 재발행해서 재배정이 스킵되는 것까지 검증했다.
  파티션 key로 인한 "순서 보장"과 "멱등성"은 서로 다른 문제라는 걸 이 과정에서 분리해서 이해했다.
- **Poison message 격리 (재시도 + DLT)** — 왜: 컨슈머가 계속 실패하는 메시지 하나 때문에 같은
  파티션의 나머지 메시지 처리까지 막힐 수 있다는 걸 실제로 재현해보고 싶었다. 배운 것:
  `DefaultErrorHandler` + `ExponentialBackOffWithMaxRetries`(1s→2s→4s→8s, 4회) 이후에도 실패하면
  `DeadLetterPublishingRecoverer`로 `<topic>-dlt` 토픽에 격리하도록 구성. 일부러 잘못된 JSON
  메시지를 직접 publish해서 DLT로 격리되는 것과, 뒤따르는 정상 메시지는 막히지 않는 것을 확인했다.
- **분산 환경에서 로그 하나로 흐름 추적하기** — 왜: 서비스가 4~5개로 늘어나니 로그가 흩어져서
  "이 예약 하나의 처리 흐름"을 따라가기가 어려워졌다. Zipkin 같은 분산 트레이싱 없이 최소한의
  방법으로 해결할 수 있는지 궁금했다. 배운 것: Kafka 메시지 key(=reservationId)를 컨슈머 쪽에서는
  `RecordInterceptor`로, Saga 시작점(예약 생성)에서는 수동으로 MDC에 넣어 로그 패턴에 노출시켰다.
  여기에 더해 각 서비스 로그를 Loki + Grafana로 중앙 수집하도록 구성했는데, `loki-logback-appender`는
  `application.yml`이 아니라 `logback-spring.xml`에서 `class` 속성을 명시해야만 동작하고 그렇지
  않으면 Joran이 조용히 no-op 한다는 걸 직접 겪으며 트러블슈팅했다.
- **API Gateway** — 왜: 클라이언트가 서비스마다 다른 포트/경로를 알아야 하는 게 불편했고, 게이트웨이가
  실제로 무엇을 대신 처리해주는지 이해하고 싶었다. 배운 것: Spring Cloud Gateway로 라우팅 규칙을
  구성하면서 어떤 서비스가 실제 비즈니스 REST API를 외부에 노출해야 하는지(reservation-service만
  해당, vehicle/payment는 actuator만, notification은 아예 웹 서버 없음)를 먼저 구분한 다음에야
  라우팅 설계가 가능하다는 걸 알게 됐다.

다음 학습 예정(Step 6, 선택)은 지금 직접 만든 폴링 퍼블리셔를 Debezium CDC로 바꿔보면서 "라이브러리가
정확히 무엇을 대신 처리해주는지" 비교해보는 것과, Kafka Streams로 실시간 집계/모니터링을 붙여보는 것이다.

## 알려진 한계 (학습 진행에 따라 다룰 예정)

- ~~Kafka는 at-least-once 전달이라 컨슈머가 같은 이벤트를 중복 수신할 수 있는데, 아직 멱등성
  처리(eventId 기준 중복 방지)가 없습니다.~~ **해결됨**: 각 서비스(reservation/vehicle/payment)에
  `ProcessedEvent`(eventId 처리 원장)를 추가해, Saga 이벤트 핸들러가 이미 처리한 eventId면
  비즈니스 로직을 다시 실행하지 않고 건너뜁니다. 같은 메시지를 재발행해서 재배정이 스킵되는 것까지
  검증했습니다.
- (추가) 컨슈머가 메시지 처리에 실패했을 때의 격리 전략도 추가됐습니다: 지수 백오프 재시도
  (1s→2s→4s→8s) 후에도 계속 실패하는 메시지(poison message)는 Dead Letter Topic(`<topic>-dlt`)으로
  격리해, 그 메시지 하나 때문에 파티션의 나머지 메시지 처리가 막히지 않게 합니다
  (`KafkaListenerConfig`, 서비스별 `config` 패키지).
- OutboxPublisher(발행 쪽)가 Kafka 발행에 실패하면 해당 폴링 배치를 중단하고 다음 주기에 재시도하는
  단순한 재시도 전략입니다. 별도의 백오프/DLT는 없습니다 — 위 항목은 **컨슈머(수신) 쪽** 재시도/DLT이고,
  발행 쪽은 아직 이 단순한 전략 그대로입니다.
- `Reservation`의 상태 전이 메서드(`confirm()`, `cancel()` 등)에 "현재 상태가 이 전이를 허용하는지"
  검증하는 가드가 없습니다. 정상 흐름에서는 문제없지만, 늦게 도착한 이벤트나 재처리 상황에서
  이미 확정된 예약이 뒤늦은 이벤트로 다시 상태가 바뀔 수 있습니다 (개발 중 실제로 이 문제로
  재현: 재시작 전 남아있던 오래된 이벤트가 재생되면서 이미 `CONFIRMED`였던 예약이 뒤늦게
  `CANCELLED`로 정정된 사례가 있었음 — 결과적으로는 올바른 최종 상태였지만, 상태 가드가 있었다면
  더 명시적으로 처리됐을 것). 멱등성 처리는 별도로 해결됐고, 이 항목은 아직 미해결입니다.
- payment-service의 결제 성공/실패는 실제 PG 연동이 아니라 금액 임계값 기반의 단순 시뮬레이션
  입니다 (`app.payment.fail-above-amount`).
