# MSA Kafka/Outbox 스터디 프로젝트

## 목적
- MSA 서비스 분리, Kafka 이벤트 기반 통신, Outbox 패턴, Saga(Choreography) 학습이 목적인
  **학습용** 프로젝트다.
- 학습이 목적이므로 프레임워크/라이브러리가 대신 처리해주는 부분도 "왜 필요한지" 이해할 수
  있게 직접 구현한다. 예: Outbox 폴링 퍼블리셔를 Debezium 없이 `@Scheduled`로 직접 구현.
  이 원칙 때문에 Outbox 엔티티/퍼블리셔는 서비스마다 각자 구현하고, `common` 모듈에
  숨겨진 라이브러리로 추상화하지 않는다. `common`에는 서비스 간에 실제로 공유해야 하는
  이벤트 payload 계약(스키마)만 둔다.
- 도메인은 렌터카 예약(도메인: 렌터카/보험대차/예약/결제)을 단순화한 버전.

## 진행 로드맵 (참고용, 순서대로 진행)
1. 프로젝트 구조 / 기술 스택 세팅 (완료)
2. 이벤트 스키마 설계 (완료 — `docs/event-schema.md`)
3~4. Reservation ↔ Vehicle, Outbox 패턴 포함 Kafka 이벤트 통신 (완료 — 사용자 요청으로 Outbox를
   처음부터 포함해서 3/4단계를 합쳐서 진행함)
5. Payment 서비스 추가해서 Saga 체인 완성 + 실패 시나리오(보상 트랜잭션) 구현 (완료)
6. (선택) Debezium CDC 전환, Kafka Streams 모니터링

## 아키텍처
- Database per Service: 서비스별 Postgres 인스턴스 분리 (`docker-compose.yml`의
  reservation-db / vehicle-db / payment-db). notification-service는 자체 DB 없음.
- Choreography 기반 Saga: 중앙 오케스트레이터 없이 각 서비스가 이벤트를 구독하고
  자기 몫을 처리한 뒤 다음 이벤트를 발행한다.
- Outbox 패턴: 비즈니스 데이터 저장과 이벤트 저장을 같은 DB 트랜잭션으로 묶고,
  별도 폴링 퍼블리셔가 outbox 테이블을 읽어 Kafka로 발행한다 (초기엔 폴링 방식,
  추후 Debezium CDC로 전환 가능).
- Kafka 메시지 key는 항상 `reservationId`(Saga 상관관계 ID)를 사용해 같은 예약에
  대한 이벤트 순서를 파티션 단위로 보장한다. 상세 스키마는 `docs/event-schema.md` 참고.

## 서비스 구성
- `reservation-service`: 예약 생성/조회, Saga 시작점. `ReservationCreated` 발행. `VehicleAssigned`
  구독(→ PAYMENT_PENDING), `VehicleAssignFailed` 구독(→ CANCELLED), `PaymentCompleted` 구독
  (→ CONFIRMED), `VehicleReleased` 구독(→ CANCELLED, 결제 실패 보상 완료 시점). `PaymentFailed`는
  직접 구독하지 않는다 — vehicle-service의 보상이 끝났다는 신호(`VehicleReleased`)로 반응한다.
- `vehicle-service`: 차량 재고/배정 관리. `ReservationCreated` 구독 → 배정 성공/실패 발행.
  `PaymentFailed` 구독 → 배정 취소(보상) 후 `VehicleReleased` 발행.
- `payment-service`: 결제 처리(금액이 `app.payment.fail-above-amount`를 넘으면 실패로 시뮬레이션).
  `ReservationCreated` 구독(→ 로컬 스냅샷 저장, reservation-service 동기 호출 대신 필요한 데이터만
  복사), `VehicleAssigned` 구독 → 결제 시도 → `PaymentCompleted` / `PaymentFailed` 발행.
- `notification-service`: 알림(카카오톡 알림톡 흉내). 자체 DB 없이 최종 이벤트를 구독해서
  로그로 출력하는 수준으로 단순화. Outbox 패턴 대상 아님(멱등성 중요도 낮음).
- `common`: 서비스 간 공유 이벤트 payload record (`com.msastudy.common.event.*`)만 포함.
  Outbox/Kafka 설정 등 인프라 코드는 넣지 않는다.

## 코딩 컨벤션
- Spring Boot 3.4.x, Java 21 (Gradle Kotlin DSL, 멀티모듈).
- 각 서비스는 독립 실행 가능해야 한다: 서비스 간 직접 의존(REST 호출, shared DB 등) 금지,
  오직 Kafka 이벤트로만 통신한다.
- 패키지 구조는 서비스마다 `domain / repository / service / web / messaging / outbox / config`로
  통일한다.
- 로컬 개발 환경: sdkman으로 설치한 Java 21 (Temurin), Gradle은 wrapper(`./gradlew`) 사용.
  인프라(Kafka, Kafka UI, Postgres x3)는 `docker-compose up`으로 띄우고, 애플리케이션
  자체는 IntelliJ에서 로컬로 직접 실행한다.
