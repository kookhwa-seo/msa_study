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
- [x] 7. 동시 예약(차량 배정) 방어 — 같은 차량이 두 예약에 동시 배정되는 경합을 비관적/낙관적
  락으로 방어, k6로 정합성·성능 검증
- [x] 8. 파티션 수 x 컨슈머 수 처리량 실습 — vehicle-service의 실제 PaymentAuthorized 컨슈머로
  파티션/컨슈머 수를 바꿔가며 처리량 변화 측정
- [x] 9. 블로킹 vs 논블로킹 외부 API 호출 — RestClient/WebClient로 느린 외부 결제망을 호출,
  동시 요청 500명에서 처리량/레이턴시 비교
- [x] 10. 가상 스레드로 블로킹 호출 살리기 — 9번의 RestClient 버전을 가상 스레드로 재기동해
  플랫폼 스레드/논블로킹과 3자 비교

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

### 7. 동시 예약(차량 배정) 방어

"같은 vehicleId+timeSlot에 유니크 제약을 걸어달라"는 요청으로 시작했는데, 이 프로젝트의
`Reservation`에는 vehicleId가, `Vehicle`에는 timeSlot 개념 자체가 없다. 대신 실제 코드에
존재하던 진짜 레이스 컨디션을 찾아 고쳤다: `VehicleAssignmentService`가 "AVAILABLE 차량 조회 →
배정 표시"를 락 없이 하고 있어서, 같은 차종+지점에 대한 가승인이 동시에 여러 건 들어오면
**같은 물리적 차량이 서로 다른 두 예약에 동시에 배정될 수 있는** 버그였다. 유니크 제약이 아니라
**비관적 락**(`SELECT ... FOR UPDATE`)으로 고쳤고, **낙관적 락**(`@Version`) 버전도 별도
메서드로 구현해서 비교했다.

```bash
docker compose up -d vehicle-db
./gradlew :vehicle-service:concurrencyTest   # 유닛 레벨 동시성 검증
BASE_URL=http://localhost:8081 BRANCH_ID=1111010100 VU_COUNT=20 \
  k6 run infra/k6/concurrent-booking-correctness.js   # 실제 HTTP+Kafka 전체 파이프라인 검증
BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-ramp-up.js   # 성능 측정
```

**실제 실행 결과:**

- **동시 20건 요청 시 락 미적용 상태에서 중복 배정 19건 발생(20건 전부 같은 차량에 배정됨)
  → 비관적/낙관적 락 적용 후 0건.** (`VehicleAssignmentConcurrencyTest`, 유닛 레벨)
- 실제 `POST /api/reservations` → Kafka → vehicle-service 전체 파이프라인으로 재검증: 재고
  1대에 동시 20명이 예약 시도 → **정확히 1명만 성공, 19명은 정상 거절**(2회 재현).
- 부하 테스트: `POST /api/reservations` 자체의 TPS `3,296.8 req/s`, p99 레이턴시 `93.65ms`,
  에러율 `0.00%` (445,076건). **이 락은 동기 API가 아니라 비동기 Kafka 컨슈머 안에 있어서
  API 자체의 TPS/레이턴시에는 영향이 없다** — 락의 실제 비용은 유닛 테스트가 잰 처리 시간
  쪽에서 나타난다: 20건 동시 처리에 비관적 락 `57ms` vs 낙관적 락 `29ms` (진 쪽을 기다리게
  하는 비관적 락이 더 느리고, 빠르게 실패시키는 낙관적 락이 더 빠르다).

**왜 비관적 락을 프로덕션에 채택했는가**: 이 도메인에서 "같은 차종+지점을 놓고 밀리초 단위로
동시에 경합하는" 상황 자체가 드물 것으로 예상했다 — 렌터카 예약은 콘서트 티켓팅처럼 특정 순간에
수천 명이 동시에 몰리는 도메인이 아니라, 평소엔 요청이 듬성듬성 들어오고 "check-then-act" 틈을
실제로 두 요청이 동시에 비집고 들어갈 확률 자체가 낮다(이번 실습의 "20건 완전 동시"는 그 틈을
일부러 넓혀 만든 최악의 경우 스트레스 테스트지, 평상시 트래픽 패턴이 아니다). 반면 낙관적 락을
쓰면 "충돌한 요청을 어떻게 할지"를 애플리케이션이 직접 책임져야 한다 — 지금 만든
`handlePaymentAuthorizedOptimistic`은 비교 실습용이라 충돌하면 그냥 "배정 실패"로 끝내지만,
프로덕션에서 정확하게 하려면 충돌 = 재고 없음이 아니므로(다른 차량이 남아있을 수 있다) 재조회 →
재시도를 반복하는 루프, 재시도 횟수 상한, 상한을 넘었을 때의 최종 실패 처리까지 직접 구현하고
테스트해야 한다 — 비관적 락은 이 모든 걸 DB의 행 잠금 대기 한 줄로 대신 해준다. 마지막으로
29ms vs 57ms(28ms 차이)는 애초에 사용자가 기다리는 동기 응답 경로가 아니라 백그라운드
Kafka 컨슈머 안에서 일어나는 차이인 데다, 그마저도 20건이 완전히 동시에 몰리는 비현실적인
최악의 경우를 가정한 수치라 실제 트래픽에서는 이보다 훨씬 작을 가능성이 높다 — 사람이 인지할
수 있는 지연 문턱(대략 100ms대)에 한참 못 미치고, 애초에 사용자 체감 경로에도 없는 차이를
근거로 구현이 더 복잡한 낙관적 락을 택할 이유가 없다고 판단했다.

실습 도중 `@Version`을 6,875대가 이미 시딩된 테이블에 추가하면서 실제로 겪은 문제(`ddl-auto:
update`는 새 컬럼을 채워주지 않아 기존 행이 전부 `version=NULL`이 되고, Hibernate가 그 행을
갱신하려다 `NullPointerException`을 던진 것)도 함께 기록해뒀다. 상세 설계와 전체 로그는
`docs/concurrent-booking-lab.md` 참고.

### 8. 파티션 수 x 컨슈머 수 처리량 실습

vehicle-service의 실제 `PaymentAuthorized` 컨슈머를 대상으로, 파티션 수와 컨슈머(스레드) 수를
바꿔가며 10,000건을 처리하는 데 걸리는 시간을 측정했다. 배정 로직(락)은 건드리지 않고 순수
소비 처리량만 쟀다 - `kafka-lab`의 `PaymentAuthorizedFloodProducer`가 실제 스키마 그대로
`payment-events`에 직접 발행하고, 같은 프로세스 안에서 AdminClient로 lag을 추적한다.

```bash
./gradlew :kafka-lab:floodPaymentAuthorized --args="10000"
```

**실제 실행 결과:**

| 파티션 수 | 컨슈머 수 | 10,000건 처리 시간 | 처리량 |
|---|---|---|---|
| 1 | 1 | 13,480ms | 741.8 msg/s |
| 4 | 1 (파티션 증설 직후 전환 구간) | 85,833ms | 116.5 msg/s |
| 4 | 1 (정상 상태) | 12,829ms | 779.5 msg/s |
| 4 | 2 | 6,448ms | 1,550.9 msg/s |
| 4 | 4 | 4,594ms | 2,176.8 msg/s |
| 4 | 5 (파티션 수 초과) | 4,719ms | 2,119.1 msg/s |

`P1,C1`과 `P4,C1(정상 상태)`이 사실상 같다 — **컨슈머가 1개뿐이면 파티션을 아무리 늘려도
처리량은 그대로**다. 컨슈머를 2개, 4개로 늘리자 처리량이 거의 정비례로 늘었다(4개일 때
약 2.8배). 그런데 5개로 늘렸을 땐 4개일 때와 처리량이 사실상 같았다 — 로그로 직접 확인해보니
5번째 컨슈머 스레드는 파티션을 **하나도 배정받지 못하고**(`assigned: []`) 그냥 놀고 있었다.

**왜 컨슈머 수가 파티션 수를 넘으면 의미가 없는가**: Kafka 컨슈머 그룹은 "파티션 하나는 같은
그룹의 컨슈머 하나에만 배정한다"는 규칙으로 동작한다 — 두 컨슈머가 같은 파티션을 동시에
읽으면 파티션 내부 순서 보장(`docs/kafka-ordering-lab.md`)이 깨지기 때문이다. 그래서 실질적으로
병렬 처리할 수 있는 컨슈머 수는 **min(컨슈머 수, 파티션 수)**로 정해지고, 파티션 수를 넘는
컨슈머는 배정받을 파티션이 없어 그냥 대기한다 — 그만큼의 스레드/커넥션/메모리가 전부 낭비다.
파티션 수가 곧 그 토픽이 낼 수 있는 병렬 처리의 물리적 상한선이라는 뜻이다.

예상 밖의 수확도 있었다: 파티션을 1→4로 늘린 직후엔 오히려 6배 넘게 느려졌다 — 이미 떠 있던
컨슈머가 새 파티션의 존재를 바로 알아채지 못해서, 리밸런스가 실제로 일어나기 전까지 새
파티션(전체 메시지의 약 75%)에 쌓인 메시지가 안 보이는 상태였다. "파티션을 늘리면 즉시
빨라진다"가 아니라, 이미 떠 있는 컨슈머가 그 변화를 알아채는 데 지연이 있을 수 있다는 걸
로그로 직접 확인했다. 상세 설계와 전체 로그는 `docs/consumer-throughput-lab.md` 참고.

### 9. 블로킹 vs 논블로킹 외부 API 호출

Saga와 무관한 독립 서비스 2개로 실습했다: `mock-payment-gateway`(포트 8087, 요청마다
200~500ms 지연 후 응답하는 가짜 결제망)와 `http-client-lab`(포트 8088, `RestClient`로 부르는
`/blocking`과 `WebClient`로 부르는 `/nonblocking`). 둘 다 같은 Tomcat(플랫폼 스레드 풀 200개)
위에서 돈다 — 차이는 응답을 기다리는 동안 스레드를 붙잡고 있느냐뿐이다.

```bash
./gradlew :mock-payment-gateway:bootRun
./gradlew :http-client-lab:bootRun
BASE_URL=http://localhost:8088 TARGET_PATH=/blocking VU_COUNT=500 \
  k6 run infra/k6/http-client-comparison.js
```

**실제 실행 결과** (동시 사용자 50/100/200/500, 각 20초):

| 동시 사용자 | 블로킹(RestClient) | 논블로킹(WebClient) |
|---|---|---|
| 50  | 138.0 req/s, p99 504ms | 138.5 req/s, p99 503ms |
| 200 | 556.6 req/s, p99 501ms | 555.5 req/s, p99 501ms |
| **500** | **555.7 req/s, p99 1,040ms** | **1,380.0 req/s, p99 505ms** |

풀 크기(200) 이하에서는 둘 다 똑같다. **500명(풀 크기 초과)이 되자 블로킹은 TPS가 556 근처에
막히고 p99가 2배로 뛰는데, 논블로킹은 TPS가 계속 늘고(2.5배) p99도 그대로였다.**

블로킹 방식이 급격히 느려지는 이유는 응답을 기다리는 동안 요청을 맡은 스레드가 통째로 묶이기
때문이다 — 동시에 처리 가능한 요청 수가 정확히 스레드 풀 크기(200개)로 막히고, 그 이상은
스레드가 빌 때까지 큐에서 대기한다(에러는 안 나지만 계속 느려지는 상황). 논블로킹은 응답을
기다리는 동안 스레드를 반납해서, 같은 200개 스레드로도 훨씬 많은 요청을 "돌려가며" 처리할 수
있다. 상세 로그는 `docs/blocking-vs-nonblocking-lab.md` 참고.

### 10. 가상 스레드로 블로킹 호출 살리기

9번의 `/blocking`(RestClient) 코드를 한 줄도 안 고치고, `spring.threads.virtual.enabled=true`
만 켜서 Tomcat이 가상 스레드로 요청을 처리하게 만들었다.

```bash
SPRING_THREADS_VIRTUAL_ENABLED=true ./gradlew :http-client-lab:bootRun
```

**실제 실행 결과 (동시 사용자 500명 기준, 3자 비교):**

| | a) RestClient+플랫폼 스레드 | b) RestClient+가상 스레드 | c) WebClient(논블로킹) |
|---|---|---|---|
| TPS | 555.7 req/s | **1,384.0 req/s** | 1,380.0 req/s |
| p99 | 1,040ms | **503ms** | 505ms |

**b)와 c)가 오차 범위 안(0.5% 이내)에서 사실상 동일했다.** 가상 스레드는 `Thread.sleep()`이나
블로킹 소켓 읽기처럼 "기다리는" 지점에서 JDK가 그 가상 스레드를 진짜 OS 스레드(캐리어)에서
자동으로 떼어내 다른 가상 스레드가 그 캐리어를 쓰게 해준다 — 그래서 코드는 여전히 익숙한
동기 스타일인데, 실제로는 수백~수천 개의 가상 스레드가 소수의 OS 스레드만 공유해서 쓴다.
WebClient가 이벤트 루프로 얻는 이점을 가상 스레드는 "기다리는 동안 캐리어를 놓아준다"는
메커니즘으로 똑같이 얻는 셈이다.

**그래도 남는 차이**: 이번처럼 "외부 API 하나를 순서대로 기다렸다가 응답만 돌려주는" 단순한
경우엔 차이가 없었지만, 여러 외부 호출을 진짜로 동시에 조합해야 하거나(`Mono.zip` 등) 리액티브
생태계의 백프레셔/재시도 연산자가 필요한 복잡한 경우라면 여전히 WebClient가 유리하다 — 가상
스레드는 "블로킹 코드를 값싸게" 만들어줄 뿐, 블로킹 코드를 논블로킹 코드로 바꿔주는 건 아니기
때문이다. 상세 로그와 분석은 `docs/virtual-thread-http-lab.md` 참고.

### 11. POST /api/reservations Rate Limiting

인기 지점이 갑자기 풀려서 순간적으로 요청이 몰리는 상황을 가정했다. Redis 토큰 버킷
(`TokenBucketRateLimiter` + `token_bucket.lua`, IP별 버킷, capacity=50/refill=50 per sec)으로
`POST /api/reservations`만 제한한다 — 버킷이 비면 즉시 `429`를 반환한다. "읽고 계산하고
쓰기"는 Lua 스크립트로 Redis 안에서 원자적으로 실행해 동시 요청 간 경합을 막는다
(`docs/concurrent-booking-lab.md`의 check-then-act 레이스와 같은 종류의 문제).

```bash
./gradlew :reservation-service:bootRun   # app.rate-limit.enabled 기본값 true
BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-spike.js
```

**실제 실행 결과** (500명이 램프업 없이 동시에 1건씩 요청):

| | 적용 전 | 적용 후 |
|---|---|---|
| 성공(201) | 500 / 500 | 92 / 500 |
| 거절(429) | 0 / 500 | 408 / 500 |
| 성공 응답 평균 | 2.09s | 424.57ms |
| 성공 응답 p99 | **2.69s** | **487.87ms** |

적용 전은 500건 전부를 성공시켰지만, 그 대가로 모든 요청이 함께 느려졌다(p99 2.69초) —
Tomcat 스레드 풀과 DB 커넥션이 감당 못 할 트래픽을 억지로 밀어넣어 대기열이 길어진
것이고, 트래픽이 조금만 더 몰렸다면 타임아웃/커넥션 고갈로 전체가 무너질 수 있는
상황이다. 적용 후는 버킷 용량만큼만 즉시 처리하고 나머지는 429로 빠르게 쳐냈다 — 성공한
92건의 응답 시간은 424ms대로, 적용 전 평균보다 5배 가까이 빨라졌다. "몰려드는 요청을
전부 받아내려는 것"과 "감당할 수 있는 만큼만 받아 그 안에서는 서비스를 지키는 것" 사이의
트레이드오프를 직접 만들어본 것이다 — 429를 받은 클라이언트가 재시도해도 되는 상황이라면
후자가 시스템 전체를 더 안전하게 지킨다. 상세 로그는 `docs/rate-limiting-lab.md` 참고.

### 12. 가용 차량 수 Redis 캐싱

`GET /api/branches/{branchId}/stock`(차종별 가용 대수)을 Redis에 cache-aside로 캐싱했다
(`availability:{vehicleType}:{branchId}`, TTL 10초). JPA 영속성 컨텍스트는 트랜잭션 하나
안에서만 유효해서 "여러 요청이 값을 공유해서 본다"는 이번 요구와는 범위가 다르다 — 그래서
Redis처럼 요청 경계 밖에서 공유되는 캐시가 필요했고, 그 대신 "누가 무효화할지"를 직접
챙겨야 한다. 실제 재고가 바뀌는 배정/보상 시점(`VehicleAssignmentService`)에 해당 키를
지우는데, **커밋 이후에** 지우도록 미뤘다 — 커밋 전에 지우면 그 사이 다른 요청이 조회해서
"아직 커밋 전인" 값을 캐시에 다시 채워버리는 race가 있기 때문이다.

```bash
APP_CACHE_ENABLED=false ./gradlew :vehicle-service:bootRun   # 적용 전(비교용)
./gradlew :vehicle-service:bootRun                            # 적용 후(기본값)
BASE_URL=http://localhost:8082 BRANCH_ID=3014010100 \
  k6 run infra/k6/availability-cache-comparison.js
```

**실제 실행 결과** (같은 지점을 100 VU가 20초간 반복 조회):

| | 적용 전 | 적용 후 |
|---|---|---|
| TPS | 525.99 req/s | 551.75 req/s |
| p99 | 1.03s | 1.15s |
| **DB 쿼리 수** | **31,695** | **263** |

TPS/p99는 거의 그대로였다 — 이 엔드포인트의 DB 작업이 인덱스 탄 단순 `COUNT` 쿼리라 원래도
싸고, 로컬 환경에서는 HTTP 루프백 왕복이 응답 시간의 대부분을 차지해서 "DB 왕복"이 "Redis
왕복"으로 바뀌어도 체감 차이가 없었다. 대신 **DB 쿼리 수는 99.2% 줄었다(31,695 → 263)** —
지금은 로컬 단일 인스턴스라 이게 응답 시간으로 안 느껴지지만, DB가 여러 서비스와 공유되거나
쿼리가 더 무겁거나 인스턴스가 여러 개라면 이 감소가 곧 DB 커넥션 풀 고갈 위험 감소·DB CPU
사용률 감소로 직결된다 — "캐싱은 항상 응답 시간을 줄여준다"가 아니라 "병목이 어디인지에
따라 캐싱 효과가 체감되는 지표가 다르다"는 걸 실측으로 확인했다. 정합성 검증(캐시 히트로
DB 쿼리 안 늘어남 + 배정 직후 stale 값이 아니라 새 값을 즉시 봄)은
`AvailabilityCacheConsistencyTest`로 확인했다. 상세 로그는 `docs/availability-cache-lab.md`
참고.

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
