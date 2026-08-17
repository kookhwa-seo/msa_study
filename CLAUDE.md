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
7. React 프런트엔드 + LLM 기반 자연어 예약 어시스턴트 추가 (완료 — mock LLM로 구조만 우선 구현)
8. 로컬 LLM(Ollama) 실제 연동 + 전국 8개 광역시 법정동 단위로 지점 데이터 확장 (완료)

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
  `PaymentFailed` 구독 → 배정 취소(보상) 후 `VehicleReleased` 발행. `branchId`는 법정동코드
  (10자리, 예: `1168010500` = 서울 강남구 삼성동)다. 서울/부산/대구/인천/광주/대전/울산/세종
  8개 광역시의 법정동 전체(1,523곳, `src/main/resources/branches.json`)를 `BranchCatalog`가
  기동 시 메모리에 올리고, `VehicleInventorySeeder`가 지점마다 차종별 0~3대를 고정 시드(42)로
  무작위 시딩한다. `GET /api/branches`(reservation-service 다음으로 두 번째 실제 비즈니스
  REST API)가 이 카탈로그를 그대로 반환해 프런트엔드의 시/도→시/군/구→동 드롭다운을 지원한다.
- `payment-service`: 결제 처리(금액이 `app.payment.fail-above-amount`를 넘으면 실패로 시뮬레이션).
  `ReservationCreated` 구독(→ 로컬 스냅샷 저장, reservation-service 동기 호출 대신 필요한 데이터만
  복사), `VehicleAssigned` 구독 → 결제 시도 → `PaymentCompleted` / `PaymentFailed` 발행.
- `notification-service`: 알림(카카오톡 알림톡 흉내). 자체 DB 없이 최종 이벤트를 구독해서
  로그로 출력하는 수준으로 단순화. Outbox 패턴 대상 아님(멱등성 중요도 낮음).
- `common`: 서비스 간 공유 이벤트 payload record (`com.msastudy.common.event.*`)만 포함.
  Outbox/Kafka 설정 등 인프라 코드는 넣지 않는다.
- `llm-service`: 자연어로 예약 요청을 받아 구조화된 필드(차종/지점/기간/금액)로 슬롯 채우기
  (slot filling)를 해주는 무상태 REST 서비스(8086). DB/Kafka 없음 — Saga에 참여하지 않고
  Choreography와 무관한 순수 UI 보조 기능이라 "서비스 간 직접 REST 호출 금지" 원칙의 예외가
  아니다(다른 서비스를 호출하지 않는다). `LlmClient` 인터페이스에 두 구현체가 있다:
  기본값인 `OllamaLlmClient`(로컬 Ollama `gemma3:4b` 실제 호출, `app.llm.provider=ollama`)와
  정규식 기반 `MockLlmClient`(`app.llm.provider=mock`, API 키/Ollama 없이 구조만 볼 때).
  직접 실험해보니 로컬 LLM이 상대 날짜 계산은 신뢰할 수 없어서 날짜는 두 구현체 모두
  `KoreanDateExtractor`(결정적 파서)에 맡긴다. 지점도 같은 이유로 LLM에게 최종 코드를
  직접 고르게 하지 않는다 — LLM/mock은 지역을 가리키는 원문 텍스트만 뽑고
  (`locationText`), `BranchMatcher`가 vehicle-service와 동일한 `branches.json`(자체 복제본,
  REST로 조회하지 않음)을 동/구/시도 순으로 단계적으로 대조해 실제 법정동코드로 확정한다.
  프런트엔드가 이 서비스의 응답으로 예약 폼을 채운 뒤, 최종 제출은 항상 reservation-service의
  정식 REST API로 한다(llm-service가 예약을 대신 생성하지 않는다).
- `frontend`: React(Vite + TypeScript) 프런트엔드. 대시보드(예약 목록 폴링 + 수동 생성 폼)와
  예약 어시스턴트(챗봇 UI) 두 화면. 모든 API 호출은 api-gateway(8085)를 거친다 — 브라우저가
  각 서비스 포트를 직접 알 필요가 없게 하는 것이 API Gateway를 둔 이유이기도 하다.

## 코딩 컨벤션
- Spring Boot 3.4.x, Java 21 (Gradle Kotlin DSL, 멀티모듈).
- 각 서비스는 독립 실행 가능해야 한다: 서비스 간 직접 의존(REST 호출, shared DB 등) 금지,
  오직 Kafka 이벤트로만 통신한다.
- 패키지 구조는 서비스마다 `domain / repository / service / web / messaging / outbox / config`로
  통일한다.
- 로컬 개발 환경: sdkman으로 설치한 Java 21 (Temurin), Gradle은 wrapper(`./gradlew`) 사용.
  인프라(Kafka, Kafka UI, Postgres x3)는 `docker-compose up`으로 띄우고, 애플리케이션
  자체는 IntelliJ에서 로컬로 직접 실행한다.
