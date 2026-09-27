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
5. Payment 서비스 추가해서 Saga 체인 완성 + 실패 시나리오(보상 트랜잭션) 구현 (완료 — 이후 "결제 안 된
   주문이 차량을 잡으면 안 된다"는 지적으로 결제 순서를 가승인 → 배정 → 매입으로 재설계함)
6. (선택) Debezium CDC 전환, Kafka Streams 모니터링
7. React 프런트엔드 + LLM 기반 자연어 예약 어시스턴트 추가 (완료 — mock LLM로 구조만 우선 구현)
8. 로컬 LLM(Ollama) 실제 연동 + 전국 8개 광역시 법정동 단위로 지점 데이터 확장 (완료)

## 대용량 트래픽 안정성 실습 로드맵 (참고용, 순서대로 진행)
위 1~8단계로 서비스 분리/이벤트 통신 골격을 완성한 뒤, 트래픽이 커져도 버티는지를 다루는
후속 실습. README.md의 "대용량 트래픽 안정성 실습" 섹션에 진행 상황과 결과를 정리한다.
1. Kafka 순서 보장 실습: 파티션 2개 이상에서 순서 깨지는 케이스 재현 + 로그로 원인 확인
   (완료 — `kafka-lab` 모듈, `docs/kafka-ordering-lab.md`)
2. 동시 결제 요청 방어: reservationId unique constraint + 예외 캐치 (완료 — `payment.reservation_id`
   UK, `PaymentService.handleReservationCreated`). Redisson 분산락 비교는 검토 후 의도적으로 보류함:
   이 케이스는 DB unique constraint 하나로 이미 정확하고 인스턴스가 늘어나도 안전해서(Postgres가
   단일 심판 역할), 분산락을 얹으면 오히려 복잡도만 늘어난다는 결론 — 사용자가 설명을 듣고 직접
   스킵하기로 판단함. README.md "대용량 트래픽 안정성 실습" 2번 항목 참고.
3. 데드락 재현·방지: 두 리소스를 반대 순서로 잠그는 코드로 재현 → 락 순서 통일로 해결
   (완료 — `deadlock-lab` 모듈, `docs/deadlock-lab.md`. vehicle-db의 실제 `vehicle` 테이블 두
   행을 순수 JDBC `SELECT ... FOR UPDATE`로 반대 순서로 잠가 실제 Postgres 데드락을 재현)
4. 오토스케일링 실습: 로컬 k8s(HPA) + k6 부하 테스트로 스케일 아웃/인 관찰 (완료 — kind 클러스터에
   reservation-service를 컨테이너로 배포, `infra/k8s/reservation-service/`, `docs/autoscaling-lab.md`.
   실제로 1->5->1개 스케일 아웃/인을 kubectl로 관찰함: 스케일 아웃 20초, 스케일 인 2분 13초)
5. 가상 스레드(JDK21+) 벤치마크: 플랫폼 스레드 대비 처리량 비교 (완료 — `vthread-lab` 모듈,
   `docs/vthread-lab.md`. I/O-bound에서 27.7배, CPU-bound에서는 거의 차이 없음(1.09배)을 실측)
6. 서비스 레지스트리(Eureka) 도입 (완료 — `eureka-server` 모듈, `docs/service-registry-lab.md`.
   디스커버리가 필요한 지점을 "API Gateway -> reservation-service"로 한정해서 Saga의 Kafka-only
   원칙은 건드리지 않았다. reservation-service 2개 인스턴스 등록, 로드밸런싱 라운드로빈 실측,
   `kill -9`로 하트비트 기반 등록 해제 + 페일오버까지 검증 - 레지스트리 등록 해제(19초)와 실제
   트래픽 차단(131초) 사이의 캐시 지연을 실측한 게 이 실습의 핵심 발견)
7. 동시 예약(차량 배정) 방어 (완료 — `docs/concurrent-booking-lab.md`. 요청은 "vehicleId+
   timeSlot 유니크 제약"이었지만 이 프로젝트 도메인엔 그 필드들이 없어서, 실제로 존재하던
   레이스 컨디션(`VehicleAssignmentService`가 같은 차량을 두 예약에 동시 배정할 수 있던 버그)을
   찾아 비관적/낙관적 락으로 고쳤다. `VehicleAssignmentConcurrencyTest`로 20건 동시 요청 시
   중복 배정 19건(락 없음) -> 0건(락 적용)을 실측, k6로 실제 HTTP+Kafka 파이프라인까지
   재검증(재고 1대에 20명 동시 요청 -> 1명만 성공). `Vehicle`에 `@Version`을 기존 시딩 데이터가
   있는 테이블에 추가하면서 겪은 백필 문제도 기록)
8. 파티션 수 x 컨슈머 수 처리량 실습 (완료 — `docs/consumer-throughput-lab.md`. vehicle-service의
   실제 PaymentAuthorized 컨슈머로 측정: 컨슈머 1개면 파티션을 늘려도 처리량 그대로(741.8 ->
   779.5 msg/s), 컨슈머를 2/4개로 늘리면 거의 정비례(최대 2,176.8 msg/s), 컨슈머가 파티션
   수(4개)를 넘으면(5개) 5번째는 파티션을 아예 못 받고 노는 것까지 로그로 확인. 덤으로 파티션을
   늘린 직후 컨슈머가 새 파티션을 바로 못 알아채 일시적으로 6배 느려지는 현상도 실측)
9. 블로킹 vs 논블로킹 외부 API 호출 (완료 — `docs/blocking-vs-nonblocking-lab.md`. 독립 서비스
   `mock-payment-gateway`(가짜 결제망, 200~500ms 지연) + `http-client-lab`(RestClient `/blocking`
   vs WebClient `/nonblocking`). 동시 500명(풀 크기 200 초과)에서 블로킹 555.7 req/s·p99 1040ms
   vs 논블로킹 1,380.0 req/s·p99 505ms)
10. 가상 스레드로 블로킹 호출 살리기 (완료 — `docs/virtual-thread-http-lab.md`. 9번의
    `/blocking` 코드를 그대로 두고 `spring.threads.virtual.enabled=true`만 켜서 동시 500명에서
    1,384.0 req/s·p99 503ms까지 향상 - WebClient와 오차 범위 안에서 동일)
11. POST /api/reservations Rate Limiting (완료 — `docs/rate-limiting-lab.md`. Redis 토큰 버킷
    (`TokenBucketRateLimiter` + `token_bucket.lua`, IP별, capacity=50/refill=50/s)을
    `RateLimitFilter`로 적용, 버킷이 비면 즉시 429. 500명 동시 스파이크 실측: 미적용 시
    500/500 성공하지만 p99 2.69s, 적용 시 92/500만 성공(408건 429)하고 성공 응답 p99는
    487.87ms — "전부 받아내려는 것"과 "감당할 수 있는 만큼만 받아 서비스를 지키는 것" 사이의
    트레이드오프를 실측으로 확인)
12. 가용 차량 수 Redis 캐싱 (완료 — `docs/availability-cache-lab.md`. `GET /api/branches/
    {branchId}/stock`을 `availability:{vehicleType}:{branchId}` 키로 cache-aside 캐싱(TTL
    10초), 배정/보상 시 커밋 이후(`afterCommit`)에 무효화. 100 VU 반복 조회 실측: TPS/p99는
    거의 그대로(로컬 COUNT 쿼리가 이미 싸고 HTTP 루프백이 병목)였지만 DB 쿼리 수는
    31,695 -> 263(99.2% 감소) - "캐싱 효과는 병목 위치에 따라 체감되는 지표가 다르다"는
    결론. `AvailabilityCacheConsistencyTest`로 캐시 히트/무효화 정합성 검증)

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
  구독(→ PAYMENT_PENDING, 배정 완료·매입 대기), `PaymentCompleted` 구독(→ CONFIRMED),
  `PaymentAuthFailed` 구독(→ CANCELLED, 배정 전이라 보상 없이 바로 취소), `PaymentVoided` 구독
  (→ CANCELLED, 재고 부족 보상 완료 시점), `VehicleReleased` 구독(→ CANCELLED, 매입 실패 보상 완료
  시점). `VehicleAssignFailed`/`PaymentFailed`는 직접 구독하지 않는다 — 보상이 끝났다는 신호
  (`PaymentVoided`/`VehicleReleased`)로 반응한다. `Dockerfile`(호스트에서 미리 빌드한 bootJar를
  복사만 하는 최소 이미지)이 있다 — 오토스케일링 실습(`infra/k8s/reservation-service/`)용으로
  추가됨, 프로덕션 배포 파이프라인은 아직 없음. `eureka-server`에 등록되는 Eureka 클라이언트이기도
  하다(서비스 레지스트리 실습, `docs/service-registry-lab.md`) — 다른 서비스는 등록하지 않는다,
  디스커버리가 필요한 지점이 API Gateway -> reservation-service뿐이라서. 여러 인스턴스를 동시에
  띄울 수 있게 `eureka.instance.instance-id`에 포트를 포함시켰고(`SERVER_PORT` 환경변수로 포트
  변경), 어느 인스턴스가 응답했는지 확인용 `X-Instance-Port` 응답 헤더(`InstancePortHeaderFilter`)를
  추가했다 — 실습 관측 목적일 뿐 비즈니스 로직과 무관. `RateLimitFilter` + `TokenBucketRateLimiter`로
  `POST /api/reservations`에 Redis 토큰 버킷 rate limit을 적용했다(`docs/rate-limiting-lab.md`) —
  `app.rate-limit.enabled` 토글로 켜고 끌 수 있다(기본 true).
- `eureka-server`: 서비스 레지스트리(포트 8761). 이 프로젝트에서 유일하게 동기 REST 호출 경로
  (API Gateway -> reservation-service)에 관여하는 인프라이고, Saga 서비스 간 통신은 여전히
  Kafka로만 이뤄진다는 원칙과 무관하다. 로컬 실습용으로 self-preservation을 끄고 하트비트/eviction
  주기를 짧게 튜닝했다(`docs/service-registry-lab.md`).
- `vehicle-service`: 차량 재고/배정 관리. `PaymentAuthorized` 구독(가승인이 성공한 예약에만 배정 —
  `reservation-events`는 구독하지 않는다, 차종/지점은 이벤트에 실려 온다) → 배정 성공/실패 발행.
  `PaymentFailed` 구독(매입 실패, 예: 가승인 만료) → 배정 취소(보상) 후 `VehicleReleased` 발행. `branchId`는 법정동코드
  (10자리, 예: `1168010500` = 서울 강남구 삼성동)다. 서울/부산/대구/인천/광주/대전/울산/세종
  8개 광역시의 법정동 전체(1,523곳, `src/main/resources/branches.json`)를 `BranchCatalog`가
  기동 시 메모리에 올리고, `VehicleInventorySeeder`가 지점마다 차종별 0~3대를 고정 시드(42)로
  무작위 시딩한다. `GET /api/branches`(reservation-service 다음으로 두 번째 실제 비즈니스
  REST API)가 이 카탈로그를 그대로 반환해 프런트엔드의 시/도→시/군/구→동 드롭다운을 지원한다.
  `VehicleAssignmentService.handlePaymentAuthorized`(프로덕션 경로)는 `SELECT ... FOR UPDATE`
  비관적 락으로 같은 차량이 두 예약에 동시 배정되는 걸 막는다 — 같은 클래스에
  `handlePaymentAuthorizedOptimistic`(`@Version` 낙관적 락)/`handlePaymentAuthorizedWithoutLock`
  (락 없음, 버그 재현용)도 비교/테스트 전용으로 있다(`docs/concurrent-booking-lab.md`).
  `Vehicle.version`은 낙관적 락 비교용으로만 실제로 쓰인다. `PaymentEventConsumer`의
  `@KafkaListener` concurrency는 `app.vehicle.payment-events-concurrency`(기본 1) 프로퍼티로
  뺐다 - 평소 동작에는 영향 없고, 파티션 x 컨슈머 수 처리량 실습(`docs/consumer-throughput-lab.md`)
  에서 재기동할 때마다 값을 바꾸기 위한 것. `GET /api/branches/{branchId}/stock`은
  `AvailabilityCacheService`로 Redis cache-aside 캐싱(`availability:{vehicleType}:{branchId}`,
  TTL 10초)을 거친다 - `VehicleAssignmentService`가 배정/보상 커밋 이후(`afterCommit`)에
  해당 키를 무효화한다(`docs/availability-cache-lab.md`). `app.cache.enabled` 토글로
  켜고 끌 수 있다(기본 true).
- `payment-service`: Saga의 첫 단계인 가승인(authorize) → 매입(capture) 2단계 결제. 금액이
  `app.payment.fail-above-amount`를 넘으면 가승인 거절로 시뮬레이션. `ReservationCreated` 구독 →
  가승인 → `PaymentAuthorized`(다음 단계 배정에 필요한 vehicleType/branchId 포함) / `PaymentAuthFailed`
  발행. `VehicleAssigned` 구독 → 매입 → `PaymentCompleted`(가승인이 `app.payment.authorization-ttl`을
  넘겼으면 `PaymentFailed(AUTHORIZATION_EXPIRED)`). `VehicleAssignFailed` 구독 → 가승인 취소(void,
  보상) → `PaymentVoided` 발행. 예약당 결제 건은 하나여야 해서 `payment.reservation_id`에 UK를 걸고,
  중복 가승인은 사전 확인(`existsByReservationId`) + UK 위반 예외 캐치(`TransactionTemplate` 바깥에서)로
  방어한다. 결제 안 된 주문이 차량을 점유하지 않고, 재고 부족 시 보상이 환불이
  아니라 보류 해제로 끝난다.
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
- `kafka-lab`: Saga에 참여하지 않는 독립 실습 모듈. Spring 없이 순수 `kafka-clients`로 작성해
  파티션/컨슈머 그룹/오프셋 같은 Kafka 저수준 동작을 직접 다룬다. 첫 실습은 파티션 3개 토픽에서
  key 없이 보낸 이벤트가 순서 보장을 깨는 것과, `reservationId`처럼 key를 고정했을 때 순서가
  보장되는 것을 재현 (`docs/kafka-ordering-lab.md`). 이후 "대용량 트래픽 안정성 실습" 항목들도
  이 모듈 또는 해당 서비스 안에 같은 방식(직접 재현 → 원인 확인 → 해결)으로 추가된다.
  `PaymentAuthorizedFloodProducer`(`floodPaymentAuthorized` 태스크)는 실제 `PaymentAuthorizedEvent`
  스키마 그대로 `payment-events`에 대량 발행하고 같은 프로세스 안에서 AdminClient로 vehicle-service
  컨슈머 그룹의 lag을 추적한다 - 파티션 x 컨슈머 수 처리량 실습(`docs/consumer-throughput-lab.md`)용.
- `deadlock-lab`: 위와 같은 원칙의 독립 실습 모듈. Spring/JPA 없이 순수 JDBC로 vehicle-db의
  실제 `vehicle` 테이블 두 행을 `SELECT ... FOR UPDATE`로 잠근다. 두 트랜잭션이 반대 순서로
  잠그면 실제 Postgres 데드락(`SQLState 40P01`)이 나는 것과, 같은 순서로 잠그면 데드락 없이
  순서대로 처리되는 것을 재현 (`docs/deadlock-lab.md`).
- `vthread-lab`: 위와 같은 원칙의 독립 실습 모듈. Spring 없이 순수 `java.util.concurrent`로
  가상 스레드와 플랫폼 스레드 풀(크기 200)을 I/O-bound(블로킹 sleep)/CPU-bound(소수 세기)
  워크로드에 각각 붙여 처리 시간을 비교 (`docs/vthread-lab.md`).
- `mock-payment-gateway`(포트 8087): 블로킹 vs 논블로킹 실습 전용 독립 서버. Saga와 무관 -
  요청마다 200~500ms 지연 후 응답해서 진짜 카드사/PG망을 흉내낸다.
- `http-client-lab`(포트 8088): 같은 실습의 비교 앱. `/blocking`(RestClient)과
  `/nonblocking`(WebClient)이 mock-payment-gateway를 각각 호출한다. `spring.threads.virtual
  .enabled`를 토글해서 가상 스레드 비교도 같은 앱으로 한다(`docs/blocking-vs-nonblocking-lab.md`,
  `docs/virtual-thread-http-lab.md`).

## 코딩 컨벤션
- Spring Boot 3.4.x, Java 21 (Gradle Kotlin DSL, 멀티모듈).
- 각 서비스는 독립 실행 가능해야 한다: 서비스 간 직접 의존(REST 호출, shared DB 등) 금지,
  오직 Kafka 이벤트로만 통신한다. **유일한 예외**: API Gateway -> reservation-service (원래부터
  Gateway는 클라이언트 요청을 백엔드로 라우팅하는 동기 호출 진입점이었고, 서비스 레지스트리
  실습에서 이 경로만 고정 주소 대신 Eureka 디스커버리로 바꿨다 — Saga 참여 서비스끼리는 여전히
  서로 직접 호출하지 않는다).
- 패키지 구조는 서비스마다 `domain / repository / service / web / messaging / outbox / config`로
  통일한다.
- 로컬 개발 환경: sdkman으로 설치한 Java 21 (Temurin), Gradle은 wrapper(`./gradlew`) 사용.
  인프라(Kafka, Kafka UI, Postgres x3)는 `docker-compose up`으로 띄우고, 애플리케이션
  자체는 IntelliJ에서 로컬로 직접 실행한다.
