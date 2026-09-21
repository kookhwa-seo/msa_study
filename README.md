# MSA Rental Study

렌터카 예약(카밀라 도메인: 렌터카/보험대차/예약/결제)을 소재로 MSA 서비스 분리, Kafka 이벤트
기반 통신, Outbox 패턴, Saga(Choreography)를 익히기 위한 **학습용** 프로젝트입니다.

자세한 설계 배경/원칙은 [`CLAUDE.md`](./CLAUDE.md), 이벤트 스키마 전체 정의는
[`docs/event-schema.md`](./docs/event-schema.md)를 참고하세요.

## 아키텍처

```
[Reservation] --ReservationCreated--> [Payment: 가승인] --PaymentAuthorized--> [Vehicle: 자동 배정]
   (Outbox)                              (Outbox)  |                             (Outbox)   |
                                                   |                                        |
                        PaymentAuthFailed ─────────┘                                        |
                                 └──> [Reservation: CANCELLED]                              |
                                                                                            |
[Reservation] <--PaymentCompleted-- [Payment: 매입(capture)] <----VehicleAssigned-----------┤
   CONFIRMED                                                                                |
                                                                                            |
[Reservation: CANCELLED] <--PaymentVoided-- [Payment: 승인 취소(void)] <--VehicleAssignFailed┘

(매입 시점에 가승인이 만료돼 실패하면)
[Payment] --PaymentFailed--> [Vehicle: 재고 반환(보상)] --VehicleReleased--> [Reservation: CANCELLED]
```

- **Database per Service**: 서비스별 Postgres 인스턴스 분리 (reservation-db / vehicle-db / payment-db).
  notification-service는 DB 없음.
- **Choreography Saga**: 중앙 오케스트레이터 없이 각 서비스가 이벤트를 구독해 자기 몫을 처리하고
  다음 이벤트를 발행. 보상은 "실패 이벤트에 직접 반응"이 아니라 "보상을 수행한 서비스가
  보상 완료 이벤트를 발행 → 그걸 받은 서비스가 최종 처리"하는 흐름으로 구현했습니다
  (재고 부족: `VehicleAssignFailed` → payment-service가 가승인 취소 → `PaymentVoided` →
  reservation-service가 최종 취소 / 매입 실패: `PaymentFailed` → vehicle-service가 재고 반환 →
  `VehicleReleased` → reservation-service가 최종 취소).
- **가승인(authorize) → 배정 → 매입(capture)**: 결제 안 된 주문이 차량을 점유하지 않도록 결제
  서비스가 Saga의 첫 단계입니다. 가승인은 카드 한도만 보류할 뿐 돈이 움직이지 않으므로, 재고가
  없을 때의 보상이 환불이 아니라 보류 해제(void) 한 번으로 끝납니다. 가승인 유효기간
  (`app.payment.authorization-ttl`, 기본 30분)이 지난 뒤 매입하려 하면 `PaymentFailed`
  (`AUTHORIZATION_EXPIRED`)로 실패하고 배정된 차량이 반환됩니다.
- **Outbox 패턴**: 비즈니스 데이터 저장 + 이벤트 저장을 하나의 DB 트랜잭션으로 묶고, 별도
  폴링 퍼블리셔(`@Scheduled`, 2초 주기)가 Kafka로 발행. Debezium 같은 CDC 도구 없이 직접
  구현했습니다 (학습 목적 — 왜 Outbox가 필요한지 코드로 확인).
- 각 서비스가 자기 `OutboxEvent` 엔티티/퍼블리셔를 독립적으로 소유합니다 (`common` 모듈에
  추상화하지 않음).
- 각 단계는 다음 단계에 필요한 데이터를 이벤트에 실어 보냅니다. payment-service는
  `ReservationCreated`의 금액으로 가승인하고, 차량 배정에 필요한 `vehicleType`/`branchId`를
  `PaymentAuthorized`에 그대로 담아 보냅니다. vehicle-service는 `reservation-events`를 구독하지
  않고 reservation-service를 조회하지도 않으며, 서로 다른 토픽의 이벤트 도착 순서에 기대는 로컬
  스냅샷도 두지 않습니다 — 이벤트 순서가 토픽 간 인과 관계로 강제됩니다.
- **프런트엔드 / LLM 어시스턴트**: React 프런트엔드는 API Gateway(8085) 하나만 알면 되고,
  브라우저는 각 서비스 포트를 직접 호출하지 않습니다. `llm-service`는 Saga에 참여하지 않는
  무상태 REST 서비스로, 자연어 메시지를 예약 필드로 변환해줄 뿐 예약을 직접 생성하지 않습니다
  (프런트엔드가 그 결과를 받아 reservation-service의 정식 API로 제출) — 그래서 "서비스 간
  직접 REST 호출 금지" 원칙과 충돌하지 않습니다.

## 서비스 구성

| 서비스 | 포트 | 담당 | 상태 |
|---|---|---|---|
| api-gateway | 8085 | 단일 진입점, `/api/reservations`·`/api/branches`·`/api/chat` 라우팅 + CORS | 구현됨 |
| reservation-service | 8081 | 예약 생성/조회, Saga 시작점 | 구현됨 |
| vehicle-service | 8082 | 차량 재고/배정(가승인 성공 후), 매입 실패 보상(재고 반환), 지점(법정동) 카탈로그 조회 API | 구현됨 |
| payment-service | 8083 | 가승인/매입/승인 취소(금액 기준 성공/실패 시뮬레이션) | 구현됨 |
| notification-service | 8084 | 알림(로그 시뮬레이션) | 구현됨 |
| llm-service | 8086 | 자연어 예약 요청 → 구조화된 필드 추출(로컬 Ollama LLM, mock으로도 전환 가능) | 구현됨 |
| frontend | 5173 (dev) | React 대시보드 + 예약 어시스턴트 UI | 구현됨 |

예약 상태 전이: `PENDING` → (가승인 성공 + 배정 성공) `PAYMENT_PENDING`(배정 완료, 매입 대기) →
(매입 성공) `CONFIRMED`, 또는 `PENDING` → (가승인 거절 / 재고 부족 보상 완료) `CANCELLED`,
`PAYMENT_PENDING` → (매입 실패 보상 완료) `CANCELLED`.
자세한 이벤트/상태 다이어그램은 [`docs/event-schema.md`](./docs/event-schema.md) 참고.

### 지점(법정동) 데이터

`branchId`는 더 이상 `SEOUL_GANGNAM` 같은 고정 상수가 아니라 **법정동코드**(10자리)입니다.
서울/부산/대구/인천/광주/대전/울산/세종 8개 광역시의 법정동 전체(1,523곳)를
[국토교통부 전국 법정동](https://www.data.go.kr/data/15063424/fileData.do) 공식 데이터
([code.go.kr 법정동코드목록조회](https://www.code.go.kr/stdcode/regCodeL.do)에서 조회, 이용허락범위
제한 없음)에서 가져와 `vehicle-service`·`llm-service` 양쪽에 `branches.json`으로 각각 번들했습니다
(서비스 독립 실행 원칙 때문에 REST로 서로 조회하지 않고 복제했습니다). 조회 시점(2026-08-17
기준)에 광주광역시는 "전남광주통합특별시"로 행정구역이 통합된 상태여서, 그 안에서 원래 광주의
5개 구(동구·서구·남구·북구·광산구)만 걸러내 "광주광역시"로 표기했습니다.

- 지점 목록 전체 조회: `GET /api/branches` (vehicle-service, 게이트웨이로는 `/api/branches`)
- 지점별 재고 조회: `GET /api/branches/{branchId}/stock` → `{"COMPACT": 2, "SUV": 0, "VAN": 1}`처럼
  차종별 `AVAILABLE` 대수를 반환합니다. 프런트엔드가 예약을 만들기 전에 재고를 미리 보여줘서,
  재고 없는 조합으로 예약해 배정 실패로 끝나는 걸 피하게 해줍니다.
- 차량 재고는 지점(동)마다 차종별로 0~3대를 무작위(고정 시드 42, 재기동해도 항상 같은 분포)로
  시딩합니다 — 그래서 특정 지점/차종 조합은 처음부터 재고가 0일 수 있고, 이게 배정 실패
  시나리오를 자연스럽게 재현합니다.

### 차량 모델(model)

예약에는 차종(`vehicleType`: COMPACT/SUV/VAN) 외에 실제 모델명(`model`, 예: "쏘나타", "카니발")도
자유 텍스트로 입력·저장됩니다. `reservation-service`의 `Reservation`에만 있는 필드로, Kafka 이벤트
payload에는 포함하지 않습니다(vehicle-service/payment-service는 배정·결제 로직에 모델명이 필요
없어서, 공유 계약을 늘리는 대신 이 서비스 안에만 두었습니다). 챗봇에서는 "쏘렌토로 렌트하고
싶어"처럼 모델명만 말해도 LLM이 그 모델이 어떤 차종인지 상식으로 유추해서 `vehicleType`까지
같이 채워줍니다. **주의**: 모델명은 정보성 필드일 뿐, 실제 차량 배정(재고 매칭)은 여전히
`vehicleType` + `branchId` 기준입니다 — 1,523개 지점마다 모델별 재고를 따로 관리하지는 않습니다.

## 기술 스택

- Spring Boot 3.4, Java 21 (Gradle Kotlin DSL 멀티모듈)
- Kafka (KRaft 모드, Zookeeper 없음), Kafka UI
- PostgreSQL 16 (서비스별 분리)
- Spring Cloud Gateway (API Gateway), React 19 + TypeScript + Vite (프런트엔드)
- Loki + Grafana (중앙 로그 수집)
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

`llm-service`는 기본값(`app.llm.provider=ollama`)으로 로컬 [Ollama](https://ollama.com)를 호출합니다.
Ollama 없이 정규식 기반 mock만 쓰려면 `llm-service/src/main/resources/application.yml`의
`app.llm.provider`를 `mock`으로 바꾸면 됩니다.

```bash
brew install ollama
ollama serve                 # 별도 터미널에서 계속 실행
ollama run gemma3:4b         # 최초 1회 모델 다운로드(~3.3GB)
```

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
./gradlew :api-gateway:bootRun
./gradlew :llm-service:bootRun
```

기동 확인 (notification-service는 웹 서버가 없어 헬스체크 엔드포인트가 없습니다 — 로그로 확인):

```bash
curl http://localhost:8081/actuator/health
curl http://localhost:8082/actuator/health
curl http://localhost:8083/actuator/health
curl http://localhost:8085/actuator/health   # api-gateway
curl http://localhost:8086/actuator/health   # llm-service
```

vehicle-service는 최초 기동 시 8개 광역시 1,523개 법정동 전체에 차종별 재고를 자동으로
시딩합니다(위 "지점(법정동) 데이터" 참고). payment-service는 `app.payment.fail-above-amount`
(기본 1,000,000원)를 초과하는 예약을 가승인 실패로 시뮬레이션합니다
(`payment-service/src/main/resources/application.yml`).

### 3. 프런트엔드 실행

```bash
cd frontend
npm install   # 최초 1회
npm run dev
```

http://localhost:5173 에서 접속합니다. 모든 API 호출은 api-gateway(8085)를 거치므로
api-gateway와 위 백엔드 서비스들이 먼저 떠 있어야 합니다. `frontend/.env.development`의
`VITE_API_BASE_URL`로 게이트웨이 주소를 바꿀 수 있습니다.

- **대시보드**(`/`): 예약 생성 폼(고객 ID/차종/모델명/지점/기간/금액) + 예약 목록(2초 폴링).
  지점은 시/도 → 시/군/구 → 동 3단계 드롭다운으로 고릅니다. 지점을 고르면 바로 아래에 차종별
  재고 뱃지(예: "소형 2대", "SUV 0대")가 뜨고, 이것도 예약 목록과 같은 2초 주기로 계속
  갱신됩니다 — 예약을 만들고 나면 처음엔 재고가 그대로 보이다가, vehicle-service가 실제로
  배정을 끝내는 몇 초 후에 숫자가 줄어드는 걸 볼 수 있습니다(아래 "학습 회고" 참고). 재고가
  0인 차종을 선택 중이면 노란 경고 문구가 뜹니다. 제출 후에는 목록이 자동 새로고침되며 Saga가
  진행됨에 따라 상태 배지가 `PENDING` → `PAYMENT_PENDING` → `CONFIRMED`/`CANCELLED`로 바뀌는
  걸 실시간으로 볼 수 있습니다.
- **예약 어시스턴트**(`/chat`): 자연어로 예약 정보를 입력하면 llm-service(기본값: 로컬 Ollama
  `gemma3:4b`, `app.llm.provider=mock`으로 전환 가능)가 문장에서 차종/모델명/지점/기간/금액을
  추출해 우측 패널에 채워줍니다. 지점은 "왕십리", "강남구 삼성동"처럼 전국 어디든 자유롭게
  말해도 됩니다 — LLM은 언급된 지역 텍스트만 뽑고, 실제 지점 코드로 매칭하는 건 결정적인
  코드가 담당합니다. "쏘렌토로 렌트하고 싶어"처럼 모델명만 말해도 차종을 함께 유추합니다.
  대시보드와 마찬가지로 지점이 정해지면 재고 뱃지/경고가 뜹니다. 모든 정보가 모이면
  '예약 확정하기' 버튼으로 실제 예약을 생성합니다(이때도 reservation-service의 정식 API를
  그대로 사용).

## API 사용 예시

지점 코드는 `curl http://localhost:8085/api/branches`로 전체 목록을 확인할 수 있습니다.
아래 예시는 그중 실제로 존재하는 코드 몇 개를 그대로 씁니다.

### 1. 재고 있음 + 결제 성공 → 자동 확정

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-1",
    "vehicleType": "SUV",
    "model": "쏘렌토",
    "branchId": "1168010500",
    "rentalStartAt": "2026-07-10T00:00:00Z",
    "rentalEndAt": "2026-07-12T00:00:00Z",
    "totalAmount": 200000
  }'
```

`branchId: 1168010500`는 서울특별시 강남구 삼성동입니다. 응답의 `reservationId`로 상태를 조회하면
수 초 내(outbox 폴링 주기 2초 x 왕복 홉 수) `PENDING` → `PAYMENT_PENDING`(가승인 + 차량 배정
완료, 매입 대기) → `CONFIRMED`(매입 완료)로 바뀝니다.

```bash
curl http://localhost:8081/api/reservations/{reservationId}
```

### 2. 재고 없음 → 가승인 취소 후 최종 취소

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-2",
    "vehicleType": "SUV",
    "model": "투싼",
    "branchId": "1111010100",
    "rentalStartAt": "2026-07-10T00:00:00Z",
    "rentalEndAt": "2026-07-12T00:00:00Z",
    "totalAmount": 300000
  }'
```

`branchId: 1111010100`(서울특별시 종로구 청운동)은 고정 시드로 시딩했을 때 SUV 재고가 0으로
나오는 지점입니다. 가승인은 성공하지만 vehicle-service가 재고를 찾지 못해 `VehicleAssignFailed`를
발행하고, payment-service가 가승인을 취소(void)하며 `PaymentVoided`를 발행합니다. reservation-service가
이 보상 완료 이벤트를 소비해 `PENDING` → `CANCELLED`로 전환하며 `ReservationCancelled`
이벤트를 다시 발행합니다. (다른 지점/차종 조합도 재고가 0일 수 있습니다 — vehicle-db에서
직접 확인하거나 `/api/branches`와 대시보드 목록을 같이 보면 재현하기 쉽습니다.)

### 3. 가승인 실패(금액 초과) → 배정 시도 없이 즉시 취소

`totalAmount`가 `app.payment.fail-above-amount`(기본 1,000,000)를 넘으면 가승인이 거절됩니다.

```bash
curl -X POST http://localhost:8081/api/reservations \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-3",
    "vehicleType": "VAN",
    "model": "카니발",
    "branchId": "1168010500",
    "rentalStartAt": "2026-08-01T00:00:00Z",
    "rentalEndAt": "2026-08-05T00:00:00Z",
    "totalAmount": 2000000
  }'
```

흐름: payment-service가 `PaymentAuthFailed` 발행 → reservation-service가 `PENDING` →
`CANCELLED`로 전환. 가승인이 거절됐으니 차량 배정은 시도조차 하지 않으므로 vehicle-db의 재고에는
아무 변화가 없습니다(이전 구조에서는 배정 후 결제 실패 → 재고 반환 보상이 필요했음).

매입 단계에서 가승인이 만료돼 실패하는 경우(`PaymentFailed` → `VehicleReleased` 보상)는
`app.payment.authorization-ttl`을 아주 짧게(예: `PT0S`) 설정하고 예약하면 재현할 수 있습니다.
이때는 배정됐던 차량이 다시 `AVAILABLE`로 돌아온 것을 vehicle-db에서 확인할 수 있습니다.

## 테스트

```bash
./gradlew test
```

`ReservationServiceTest`, `VehicleAssignmentServiceTest`, `PaymentServiceTest`가 각 서비스의
핵심 로직(예약 생성/가승인/배정/매입 시 Outbox 저장, 성공/실패에 따른 상태 전이와 Outbox 이벤트
내용, 가승인 취소(void), 매입 실패 시 차량 재고 반환)을 검증합니다.

## 진행 로드맵

- [x] 1. 프로젝트 구조 / 기술 스택 세팅
- [x] 2. 이벤트 스키마 설계
- [x] 3~4. Reservation ↔ Vehicle, Outbox 패턴 포함 Kafka 이벤트 통신
- [x] 5. Payment 서비스 추가 + Saga 체인 완성 (결제 실패 시 보상 트랜잭션)
- [ ] 6. (선택) Debezium CDC 전환, Kafka Streams 모니터링
- [x] 7. React 프런트엔드 + LLM 기반 자연어 예약 어시스턴트 (mock LLM로 구조 우선 구현)
- [x] 8. 로컬 LLM(Ollama) 실제 연동 + 전국(8개 광역시) 법정동 단위 지점 데이터로 확장
- [x] 9. 차량 모델명 필드 + 지점별 실시간 재고 가시성(폴링) 추가, RAG(임베딩 매칭) 실험

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
  구독해서 결제에 필요한 최소한의 정보(`ReservationSnapshot`)만 로컬에 복제해두는 패턴을 처음
  구현해봤고(이후 가승인 구조로 바꾸면서 스냅샷 대신 이벤트에 다음 단계 데이터를 실어 보내는
  방식으로 대체), 이를 통해, "서비스 간 결합을 없앤다"는 게 실제로 어떤 트레이드오프(데이터 중복 vs 독립성)인지
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
  `PaymentFailed`를 직접 구독하지 않는 이유를 설계하면서 명확해졌다. 이후 "결제 안 된 주문이
  차량을 잡으면 안 된다"는 지적에서 출발해 순서를 가승인 → 배정 → 매입으로 바꿨다. 되돌리기
  쉬운 단계(가승인 보류)를 앞에, 되돌리기 어려운 단계(실제 청구)를 뒤에 두는 것이 Saga 순서
  설계의 원칙이라는 점과, 이벤트에 다음 단계 데이터를 실어 보내면 토픽 간 도착 순서 의존이
  사라진다는 점을 확인했다.
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
  구성하면서 어떤 서비스가 실제 비즈니스 REST API를 외부에 노출해야 하는지를 먼저 구분한 다음에야
  라우팅 설계가 가능하다는 걸 알게 됐다. (당시엔 reservation-service만 REST API가 있었는데, 8단계에서
  vehicle-service에 지점 조회 API가 추가되면서 이 구분이 실제로 한 번 더 쓰였다.)
- **로컬 LLM 연동과 "LLM + 결정적 도구" 패턴** — 왜: mock으로 구조만 잡아둔 걸 실제 로컬 LLM(Ollama
  `gemma3:4b`)으로 바꾸면 뭐가 달라지는지, 그리고 실제 LLM이라고 모든 걸 다 잘하는 건 아니라는 걸
  직접 확인하고 싶었다. 배운 것: 4B급 로컬 모델은 "다음주 화요일" 같은 날짜 계산이나 (예를 들어)
  "동성로3가" 같은 정확한 지점 코드를 1,500여 개 후보 중에서 직접 고르는 일은 신뢰할 수 없었다.
  그래서 날짜는 결정적 파서(`KoreanDateExtractor`)에, 지점은 LLM이 "지역 텍스트만 추출"하고
  결정적 매칭기(`BranchMatcher`, 동 이름 → 없으면 구 이름 → 없으면 시/도 이름 순으로 단계적으로
  완화하며 대조)가 실제 코드로 변환하는 역할 분담을 하게 됐다 — LLM은 자유로운 자연어 이해에,
  정확한 계산/매칭은 코드에 맡기는 게 실무에서도 흔한 패턴이라는 걸 직접 겪으며 이해했다.
- **행정구역 데이터로 실제 서비스 범위 넓히기** — 왜: 지점이 강남/해운대 두 곳뿐이면 "왕십리"
  같은 흔한 지명도 인식을 못 한다는 걸 사용자 피드백으로 알게 됐고, 실제 정부 공개 데이터로
  전국 단위까지 확장하면 뭐가 달라지는지 보고 싶었다. 배운 것: 국토교통부 법정동 데이터를 받아
  8개 광역시(1,523개 동)로 필터링해 vehicle-service(재고 시딩 + 조회 API)와 llm-service(지점
  매칭)에 각각 복제했다. 이 과정에서 "강남"이라는 말만으로는 실제 법정동 이름(강남구엔 '강남동'이
  없고 역삼동/삼성동 등으로 구성)과 매칭이 안 된다는 걸 발견해서, 동 이름 → 구 이름 → 시/도 이름
  순으로 단계적으로 완화하는 매칭 전략을 직접 설계해야 했다. 또 조회 시점 기준으로 광주광역시가
  "전남광주통합특별시"로 행정구역이 통합돼 있어서, 공식 데이터라고 해도 이름이 고정돼 있지 않고
  시점에 따라 달라진다는 것도 실제로 겪었다.

- **재고 가시성 기능으로 eventual consistency 직접 체감** — 왜: "PENDING 예약을 UI에서 바로
  PAYMENT_PENDING으로 만들 수 있게 해달라"는 요청에서 출발했는데, 실제 의도를 확인해보니
  "재고 있는 조합을 미리 알고 싶다"는 거였다(Saga 상태를 UI가 직접 바꾸는 건 이 프로젝트의
  핵심 원칙과 충돌해서 제외). 배운 것: 지점별 재고 조회 API(`GET /api/branches/{id}/stock`)를
  만들고 프런트에서 처음엔 한 번만 조회하게 했더니, "예약을 만들어도 재고 숫자가 안 바뀐다"는
  피드백을 받았다 — 이유를 파고들어 보니 프런트 문제가 아니라, **예약 생성 시점엔 아직 실제
  차량 배정이 안 일어난 상태**라는 걸 다시 확인하게 됐다(reservation-db는 즉시 바뀌지만
  vehicle-db는 vehicle-service가 Outbox 이벤트를 처리할 때까지 몇 초 그대로다 — 이게 바로
  eventual consistency). 예약 목록과 같은 2초 폴링을 재고 조회에도 적용해서, 재고 숫자가
  실제 배정 처리 후 줄어드는 걸 볼 수 있게 했다.
- **RAG(임베딩 매칭) 실험 — 안 되는 것도 구현 전에 확인하는 습관** — 왜: 지점 매칭이 "왕십리"
  같은 동 이름 자체는 인식해도 "코엑스 근처" 같은 랜드마크는 인식 못 하길래, 임베딩으로 의미
  기반 검색을 붙이면 해결될지 궁금했다. 배운 것: 전체 기능을 만들기 전에 로컬에서 먼저
  검증해봤는데(Ollama `nomic-embed-text`로 "코엑스 근처"와 후보 동 이름들의 코사인 유사도를
  직접 계산), 정답(삼성동)이 무관한 동보다도 낮게 나왔다. 채팅 모델(`gemma3:4b`)에게 "코엑스가
  어느 구에 있어?"라고 직접 물어봐도 틀린 답(송파구, 정답은 강남구)이 나와서, 문제가 임베딩
  방식 자체가 아니라 **로컬 모델들이 애초에 이 정도로 구체적인 지리 지식을 갖고 있지 않다**는
  걸 확인했다. RAG는 "검색 대상에 정답이 있어야" 의미가 있는데, 여기선 검색 대상(1,523개
  동 이름)에 "코엑스"라는 정보 자체가 없어서 임베딩을 아무리 잘 써도 풀리지 않는 문제였다.
  기능을 다 만들고 나서 안 되는 걸 알기보다, 작은 스크립트로 먼저 검증해서 시간을 아꼈다 —
  날짜 계산 때(4B 모델이 상대 날짜를 못 다룸을 미리 확인) 썼던 것과 같은 습관이다.

다음 학습 예정(Step 6, 선택)은 지금 직접 만든 폴링 퍼블리셔를 Debezium CDC로 바꿔보면서 "라이브러리가
정확히 무엇을 대신 처리해주는지" 비교해보는 것과, Kafka Streams로 실시간 집계/모니터링을 붙여보는 것이다.

## 대용량 트래픽 안정성 실습

기존 로드맵(1~9단계)이 "MSA를 어떻게 나누고 이벤트로 연결하는가"에 집중했다면, 이 단계는 그렇게
나눈 서비스들이 **트래픽이 커져도 안정적으로 버티는가**를 다룬다. 각 항목은 실패/경합 상황을
일부러 재현해서 원인을 로그·지표로 확인한 다음 고치는 순서로 진행한다.

- [x] 1. Kafka 순서 보장 실습 — 파티션 2개 이상에서 순서 깨지는 케이스 재현 + 로그로 원인 확인
- [x] 2. 동시 결제 요청 방어 — reservationId unique constraint + 예외 캐치
- [x] 3. 데드락 재현·방지 — 반대 순서로 리소스를 잠그는 코드로 재현 → 락 순서 통일로 해결
- [x] 4. 오토스케일링 실습 — 로컬 k8s(HPA) + k6 부하 테스트로 스케일 아웃/인 관찰
- [x] 5. 가상 스레드(JDK21+) 벤치마크 — 플랫폼 스레드 대비 처리량 비교
- [x] 6. 서비스 레지스트리(Eureka) 도입 — 등록/헬스체크 기반 해제/클라이언트 사이드
  디스커버리/로드밸런싱, API Gateway 연동

### 1. Kafka 순서 보장 실습

파티션 3개짜리 토픽에서, 같은 예약(aggregate)의 이벤트를 key 없이 여러 파티션에 흩뿌렸을 때
실제로 순서가 깨지는 것과, `reservationId`로 key를 고정했을 때(실제 이 프로젝트의 방식) 순서가
보장되는 것을 순수 `kafka-clients` 기반 랩(`kafka-lab` 모듈)으로 재현했다.

```bash
docker compose up -d kafka
./gradlew :kafka-lab:run --args="broken"   # 순서 깨짐 재현
./gradlew :kafka-lab:run --args="fixed"    # reservationId 방식과 동일 - 순서 보장
```

실행 로그 발췌(20개 이벤트, "처리 완료 순서"가 "전송 순서"와 얼마나 어긋나는지):

```
[broken] 처리 완료 순서: 0 1 2 3 5 4 6 7 8 9 12 10 11 13 14 17 15 18 16 19  (역전 4건)
[fixed]  처리 완료 순서: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19  (역전 0건)
```

Kafka는 **토픽 전체가 아니라 파티션 내부의 순서만** 보장한다. 파티션을 늘려 처리량을 올리는
순간부터 "같은 예약 건은 항상 같은 key로 보낸다"는 규칙을 지키지 않으면 예외 없이 조용히
순서가 깨진다 — 이 프로젝트의 Saga 이벤트가 처음부터 `reservationId`를 key로 고정해온 이유를
반증으로 확인한 것이다. 상세 설계와 전체 로그는 `docs/kafka-ordering-lab.md` 참고.

**주의**: 이 랩은 토픽 하나 안에서만 key를 바꿔본 것이다. "`ReservationCreated`와
`VehicleAssigned`처럼 서로 다른 이벤트를 같은 key로 보내면 순서가 보장되나요?"라는 질문에는
"토픽이 같으면 맞고, 다르면 아니다"가 정답이다 — 실제로 이 둘은 서로 다른 토픽
(`reservation-events` vs `vehicle-events`)에 쌓이고, Kafka의 순서 보장은 토픽을 넘어가지
않는다. 이 프로젝트에서 그 순서가 실제로 안전한 이유는 Kafka가 아니라 **Choreography Saga의
인과관계**(다음 이벤트는 항상 이전 이벤트를 처리한 결과로만 발행된다) 때문이다. 자세한 설명은
`docs/kafka-ordering-lab.md`의 "이 랩이 다루지 않는 부분" 참고.

### 2. 동시 결제 요청 방어

같은 예약(`reservationId`)에 대한 가승인 요청이 동시에 여러 번 들어와도(중복 이벤트 재전달,
경합 등) 결제 건이 두 번 생기지 않도록 두 겹으로 막는다 (`PaymentService.handleReservationCreated`,
`Payment.reservation_id`에 걸린 `uk_payment_reservation_id` unique constraint):

1. `existsByReservationId` 사전 확인 — 순차적으로 들어온 중복을 예외 없이 조용히 스킵.
2. DB unique constraint — 1번 확인과 실제 저장 사이에 다른 트랜잭션이 끼어드는 진짜 동시 요청을
   DB가 막는다. `DataIntegrityViolationException`을 캐치해서 "그 예약의 결제가 실제로 존재하면
   동시 처리로 인한 중복"으로 판단해 무시하고, 아니면 다른 무결성 오류이므로 그대로 던져
   재시도/DLT로 넘긴다. `@Transactional` 대신 `TransactionTemplate`을 쓴 이유는 유니크 위반이
   보통 커밋 시점에 터지는데, 트랜잭션 안에서 잡으면 이미 rollback-only라 아무것도 못 하기
   때문 — 트랜잭션 경계를 메서드 안쪽으로 좁혀서 롤백이 끝난 바깥에서 처리한다.

**왜 비관적 락(`SELECT ... FOR UPDATE`)이 아니라 unique constraint인가**: 비관적 락은 "이미
존재하는 행"에만 걸 수 있다. 이 예약은 **신규 주문이라 결제 행 자체가 아직 없는 상태**라서,
비관적 락으로는 애초에 잠글 대상이 없다. Unique constraint는 행을 잠그는 게 아니라 **INSERT가
커밋되는 순간 인덱스 자체가 중복을 막아주는** 방식이라 이 문제를 겪지 않는다 — 두 트랜잭션이
동시에 같은 reservationId로 INSERT를 시도해도, 둘 다 "잠글 기존 행"이 없었던 것과 무관하게
DB가 하나만 통과시킨다. (신규 행 문제의 또 다른 정답은 행이 아니라 **식별자 자체("order:
{orderId}" 같은 키)를 잠그는 앱 레벨/분산락**이다 — Redisson 같은 도구가 여기서는 진짜
쓸모가 있다. 다만 이 프로젝트는 이미 unique constraint로 충분해서 그 경로까지 가지 않았다.)

원래는 Redisson 분산락과 비교 구현까지 계획했지만, 이 문제(하나의 테이블에 유니크 키로 표현
가능한 단순 중복 방지)는 DB unique constraint만으로 이미 정확하고, 인스턴스가 몇 개로 늘어나도
(Kafka key가 reservationId라 같은 예약의 이벤트는 항상 같은 파티션 → 같은 컨슈머 인스턴스로만
가기도 하고, 설사 여러 인스턴스가 동시에 써도 Postgres 자체가 단일 심판 역할을 한다) 안전하다는
결론에 도달해서 Redisson 도입은 보류했다. 분산락은 "DB 하나의 유니크 키로 표현 안 되는 경합"
(여러 테이블/서비스에 걸친 작업, 비싼 외부 호출을 애초에 중복 실행하고 싶지 않은 경우)에
필요한 도구이고, 지금 여기 억지로 얹으면 오히려 Redis 의존성·락 TTL 튜닝 같은 복잡도만 늘어난다
— "언제 안 써도 되는지"를 판단한 것 자체가 이 항목의 결론이다.

### 3. 데드락 재현·방지

이 프로젝트가 실제로 쓰는 vehicle-db(Postgres)의 `vehicle` 테이블에서, 두 트랜잭션이 같은
행 2개(차량A/차량B)를 `SELECT ... FOR UPDATE`로 잠그되 순서를 반대로 하면 실제로 무슨 일이
일어나는지 순수 JDBC 기반 랩(`deadlock-lab` 모듈)으로 재현했다.

```bash
docker compose up -d vehicle-db
./gradlew :deadlock-lab:run --args="broken"   # 반대 순서 - 데드락 재현
./gradlew :deadlock-lab:run --args="fixed"    # 같은 순서 - 데드락 없음
```

실행 로그 발췌:

```
[broken] tx-1: A→B 순서, tx-2: B→A 순서로 잠금
  tx-1 2차 락 획득: B (1003ms 대기)  |  tx-1 커밋 완료
  tx-2: Postgres가 데드락을 탐지해서 강제 중단 (SQLState 40P01, "deadlock detected")
  전체 소요 시간: 1038ms

[fixed]  tx-1, tx-2 모두 A→B 순서로 잠금
  tx-1 커밋 완료 → tx-2가 그 뒤를 이어 바로 커밋 완료 (둘 다 성공)
  전체 소요 시간: 25ms
```

Postgres는 데드락을 스스로 탐지해서(기본 `deadlock_timeout` 1초) 한쪽 트랜잭션을 강제로
롤백시키는 안전장치가 있지만, 그 탐지에 걸리는 시간만큼 응답이 느려지고(1038ms vs 25ms) 롤백된
트랜잭션은 애플리케이션이 재시도하거나 실패로 처리해야 한다. 해결책은 별도 락 라이브러리가
아니라 **"여러 행을 잠글 때는 항상 정해진 순서로 잠근다"는 규칙 하나**다 — fixed 모드가
보여주듯, 이 규칙만 지키면 순환 대기(circular wait) 자체가 성립할 수 없다. 상세 설계와 전체
로그는 `docs/deadlock-lab.md` 참고.

### 4. 오토스케일링 실습

로컬 kind 클러스터에 `reservation-service`를 실제로 컨테이너화해서 배포하고, HPA(CPU 사용률
50% 목표, 1~5개 파드)를 걸어둔 다음 k6로 부하를 줘서 스케일 아웃/인을 직접 관찰했다.

```bash
kind create cluster --name msa-study
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
./gradlew :reservation-service:bootJar
docker build -t reservation-service:hpa-lab -f reservation-service/Dockerfile reservation-service
kind load docker-image reservation-service:hpa-lab --name msa-study
kubectl apply -f infra/k8s/reservation-service/manifests.yaml
kubectl port-forward svc/reservation-service 18081:8081 &
k6 run infra/k8s/reservation-service/load-test.js
```

실제 `kubectl get hpa` 타임라인 발췌 (40 VU 부하를 2분간 유지):

```
17:27:26  cpu:  20%/50%   pods=1     <- 부하 시작 전
17:28:12  cpu: 228%/50%   pods=3     <- 목표치 초과, 즉시 스케일 아웃
17:28:27  cpu: 253%/50%   pods=5     <- 최대치 도달 (1->5, 약 20초)
17:29:08  cpu:  49%/50%   pods=5     <- 목표치 근처로 안정화
17:29:54  cpu:  42%/50%   pods=5->4  <- 부하 종료, 스케일 인 시작
17:31:41  cpu:  10%/50%   pods=2->1
17:32:07  cpu:  10%/50%   pods=1     <- 원래대로 복귀 (5->1, 약 2분 13초)
```

k6는 56,415건을 실패 0%로 처리했다(평균 6.28ms, 313 req/s) — HPA는 "요청이 실패/지연되는 걸
보고" 반응하는 게 아니라 **파드의 리소스 사용률을 보고** 반응한다는 걸 보여준다. **스케일
아웃은 빠르고(20초) 스케일 인은 느린(2분 13초) 게 우연이 아니라 의도적으로 다르게 설정한
정책**(`behavior.scaleUp`/`scaleDown`)이다 — 늘릴 땐 빠르게 반응하고, 줄일 땐 신중하게 반응해서
부하가 잠깐 튀었다 가라앉을 때마다 파드를 늘렸다 줄였다 반복하는 "플래핑"을 피한다. 상세 설계와
전체 로그는 `docs/autoscaling-lab.md` 참고.

### 5. 가상 스레드(JDK21+) 벤치마크

플랫폼 스레드 풀(크기 200 — Spring Boot 내장 Tomcat 기본값과 동일)과 가상 스레드(작업당 1개)로
같은 워크로드를 돌려서 비교했다 (`vthread-lab` 모듈, Spring 없이 순수 `java.util.concurrent`).

```bash
./gradlew :vthread-lab:run
```

실제 실행 결과 (10코어 머신):

```
I/O-bound (작업 10,000개, 각 50ms 블로킹 sleep)
  플랫폼 스레드: 2,691ms   가상 스레드: 97ms    -> 27.7배

CPU-bound (작업 2,000개, 각 3만 이하 소수 개수 세기)
  플랫폼 스레드:   162ms   가상 스레드: 148ms   -> 1.09배 (거의 차이 없음)
```

가상 스레드는 "I/O로 기다리는 동안 OS 스레드를 점유하지 않는다"는 게 핵심이지, 연산 자체를
빠르게 해주는 게 아니다. I/O-bound(블로킹 대기가 있는 작업)에서는 스레드 풀 크기라는 병목이
사라져서 27.7배 차이가 나지만, CPU-bound(온전히 계산만 하는 작업)에서는 어느 쪽이든 물리
코어 수 이상 동시에 실행될 수 없어서 차이가 거의 없다. 이 프로젝트의 서비스들(REST API +
JPA/JDBC 호출)은 전형적인 I/O-bound 워크로드라 이 이점을 받을 수 있는 후보지만, 지금 트래픽
규모에서는 기본 플랫폼 스레드 풀로도 병목이 없다. 상세 설계와 원인 분석은 `docs/vthread-lab.md`
참고.

### 6. 서비스 레지스트리(Eureka)

이 프로젝트에서 서비스 디스커버리가 필요한 지점은 딱 하나, **API Gateway → reservation-service**
뿐이다(Saga 내부는 여전히 Kafka로만 통신). `eureka-server`(신규 모듈)를 띄우고,
reservation-service를 **인스턴스 2개(포트 8081/8091)** 로 띄운 다음, api-gateway의 라우팅을
고정 주소(`http://localhost:8081`)에서 `lb://reservation-service`(Eureka 조회 + 로드밸런싱)로
바꿔서 등록/디스커버리/로드밸런싱/장애 시 등록 해제를 전부 직접 확인했다.

```bash
./gradlew :eureka-server:bootRun &
./gradlew :reservation-service:bootRun &                    # 인스턴스 1: 8081
SERVER_PORT=8091 ./gradlew :reservation-service:bootRun &   # 인스턴스 2: 8091
./gradlew :api-gateway:bootRun &
```

로드밸런싱 확인 (`X-Instance-Port` 응답 헤더로 실제 처리한 인스턴스 확인, 10회 연속 호출):

```
8091 8081 8091 8081 8091 8081 8091 8081 8091 8081   <- 정확히 라운드로빈
```

8091 인스턴스를 `kill -9`로 강제 종료한 뒤 실제로 관찰한 타임라인:

```
22:39:13  kill -9로 8091 강제 종료
22:39:32  Eureka 레지스트리에서 등록 해제 (19초 - 하트비트 만료)
22:40:52  게이트웨이는 여전히 죽은 8091로 절반씩 라우팅 중 (Connection refused)
22:41:24  이 시점부터 전부 8081(생존 인스턴스)로만 라우팅 - 총 131초 소요
```

**레지스트리에서 지워지는 것(19초)과 호출하는 쪽이 그걸 실제로 반영하는 것(131초) 사이에
큰 간극이 있다** — 게이트웨이 안에 Eureka 클라이언트 레지스트리 캐시, Spring Cloud
LoadBalancer의 인스턴스 목록 캐시, Eureka 서버 자체의 응답 캐시까지 여러 겹이 있기 때문이다.
"등록 해제됐으니 트래픽이 바로 끊기겠지"라고 가정하면 안 된다는 걸 숫자로 직접 확인했다 —
실무에서 재시도/서킷 브레이커를 같이 두는 이유이기도 하다. 상세 설계와 전체 타임라인은
`docs/service-registry-lab.md` 참고.

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
- payment-service의 가승인 성공/실패는 실제 PG 연동이 아니라 금액 임계값 기반의 단순 시뮬레이션
  입니다 (`app.payment.fail-above-amount`). 가승인 만료도 매입 시점에 시간만 비교하는 수준이라,
  매입 요청이 오지 않는 채로 만료되는 경우를 정리하는 스케줄러는 없습니다.
- (서비스 레지스트리 실습으로 새로 드러난 부분) `OutboxPublisher`는 리더 선출이나 락 없이
  `@Scheduled`로 단순 폴링만 합니다. reservation-service를 인스턴스 여러 개로 띄우면(6번 실습
  참고) 각 인스턴스가 같은 reservation-db의 같은 pending 이벤트를 동시에 폴링하다가 같은 이벤트를
  중복 발행할 수 있습니다 — 컨슈머 쪽 멱등성(`ProcessedEvent`)이 그 중복을 걸러주긴 하지만,
  발행 쪽 자체의 "한 이벤트는 한 인스턴스만 발행한다"는 보장은 아직 없습니다.
- ~~llm-service의 자연어 추출은 실제 LLM이 아니라 정규식/키워드 기반 mock입니다.~~ **해결됨**:
  기본값이 로컬 Ollama(`gemma3:4b`)를 실제로 호출하는 `OllamaLlmClient`로 바뀌었습니다
  (`app.llm.provider=ollama`). 정규식 기반 `MockLlmClient`는 API 키/Ollama 없이 구조만 볼 때
  쓰는 대안(`app.llm.provider=mock`)으로 남아있는데, 명시적 날짜나 "강남/해운대/서울/부산" 같은
  몇 개 키워드만 인식하고 "왕십리"처럼 목록에 없는 지명은 인식하지 못합니다 — 이게 정확히
  실제 LLM(지리 지식으로 임의의 지명을 이해)이 필요한 이유이기도 합니다.
- 지점 매칭(`BranchMatcher`)은 동 이름이 겹치는 경우(예: "송정동"은 서울 성동구에도, 부산
  해운대구에도 있습니다) 문맥을 보지 않고 법정동코드가 더 작은 쪽(대략 서울이 먼저)을 기계적으로
  고릅니다. 이미 알고 있는 시/도 정보를 활용해 disambiguate하는 건 아직 하지 않습니다.
- "코엑스", "롯데월드"처럼 동 이름이 아닌 랜드마크명은 인식하지 못합니다. 임베딩 기반 의미 검색으로
  풀어보려고 시도했지만(위 학습 회고 참고), 검증해보니 로컬 LLM/임베딩 모델 둘 다 이런 구체적인
  지리 지식을 갖고 있지 않아 포기했습니다. 정확히 풀려면 카카오/네이버 지도 같은 실제 장소 검색
  API 연동이 필요할 것으로 보입니다(아직 미착수).
- 예약의 `model`(차량 모델명)은 정보성 필드일 뿐 재고 매칭에는 쓰이지 않습니다. 실제로 지점마다
  모델별 재고를 관리하려면 1,523개 지점 × 모델 카탈로그를 새로 시딩해야 해서 범위를 의도적으로
  좁혔습니다.
