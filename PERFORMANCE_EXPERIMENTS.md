# 성능/트래픽 실습 종합 정리

`README.md`의 "대용량 트래픽 안정성 실습" 9~12번을 한 문서로 묶었다. 네 실습 모두 같은
흐름을 따른다: **재현 가능한 부하 시나리오를 k6로 만들고 → 적용 전/후를 같은 조건에서
실측 비교하고 → 왜 그런 결과가 나왔는지 설명한다.** 개별 실습의 전체 로그/코드 설명은 각
`docs/*.md` 링크를 참고.

| # | 실습 | 핵심 발견 | 상세 문서 |
|---|---|---|---|
| 1 | 블로킹 vs 논블로킹 외부 API 호출 | 동시 500명(스레드 풀 200 초과)에서 블로킹 TPS 555.7·p99 1,040ms vs 논블로킹 TPS 1,380.0·p99 505ms | `docs/blocking-vs-nonblocking-lab.md` |
| 2 | 가상 스레드로 블로킹 호출 살리기 | 코드 무변경 + `virtual.enabled=true`만으로 동시 500명에서 TPS 1,384.0·p99 503ms — 논블로킹과 오차범위 내 동일 | `docs/virtual-thread-http-lab.md` |
| 3 | Rate Limiting (토큰 버킷) | 500명 스파이크: 미적용 500/500 성공·p99 2.69s vs 적용 92/500 성공(408건 429)·성공 응답 p99 487.87ms | `docs/rate-limiting-lab.md` |
| 4 | 가용 차량 수 Redis 캐싱 | 100VU 반복 조회: TPS/p99는 거의 그대로, DB 쿼리 수 31,695 → 263(99.2% 감소) | `docs/availability-cache-lab.md` |

---

## 1. 블로킹 vs 논블로킹 외부 API 호출

**시나리오**: 외부 결제망(`mock-payment-gateway`, 200~500ms 랜덤 지연)을 `RestClient`(블로킹,
`/blocking`)와 `WebClient`(논블로킹, `/nonblocking`)로 각각 호출. 둘 다 같은 Tomcat 플랫폼
스레드 풀(200개) 위에서 돈다 — 차이는 "응답 대기 중 스레드를 붙잡고 있느냐"뿐이다.

```bash
./gradlew :mock-payment-gateway:bootRun
./gradlew :http-client-lab:bootRun
BASE_URL=http://localhost:8088 TARGET_PATH=/blocking VU_COUNT=500 \
  k6 run infra/k6/http-client-comparison.js
```

| 동시 사용자 | 블로킹 TPS | 블로킹 p99 | 논블로킹 TPS | 논블로킹 p99 |
|---|---|---|---|---|
| 50  | 138.0 req/s | 504ms | 138.5 req/s | 503ms |
| 200 | 556.6 req/s | 501ms | 555.5 req/s | 501ms |
| **500** | **555.7 req/s** | **1,040ms** | **1,380.0 req/s** | **505ms** |

풀 크기(200) 이하에서는 둘 다 동일하다. 500명(풀 크기 초과)이 되자 블로킹은 스레드 200개가
물리적 상한이 되어 TPS가 556 근처에 막히고 p99가 2배로 뛰는데(요청이 실패하지 않고 큐에서
대기), 논블로킹은 대기 중 스레드를 반납해서 같은 200개로도 TPS가 계속 늘고(2.5배) p99도
그대로였다.

## 2. 가상 스레드로 블로킹 호출 살리기

1번의 `/blocking`(RestClient) 코드를 한 줄도 안 고치고, `spring.threads.virtual.enabled=true`
만 켜서 Tomcat이 가상 스레드로 요청을 처리하게 했다. JDK 21이 블로킹 I/O 지점에서 가상
스레드를 캐리어(OS) 스레드에서 자동으로 떼어내는 메커니즘 덕분이다.

```bash
SPRING_THREADS_VIRTUAL_ENABLED=true ./gradlew :http-client-lab:bootRun
```

| 동시 사용자 500명 | a) RestClient+플랫폼 스레드 | b) RestClient+가상 스레드 | c) WebClient(논블로킹) |
|---|---|---|---|
| TPS | 555.7 req/s | **1,384.0 req/s** | 1,380.0 req/s |
| p99 | 1,040ms | **503ms** | 505ms |

b)와 c)가 오차범위(0.5% 이내)에서 사실상 동일했다 — 코드 변경 없이 가상 스레드만 켜도
논블로킹과 동등한 처리량을 얻었다. 단, 이건 "외부 API 하나를 순서대로 기다렸다가 응답만
돌려주는" 단순한 패턴에 한한 결과다. 여러 외부 호출을 진짜로 동시 조합(`Mono.zip` 등)하거나
백프레셔/재시도 같은 리액티브 연산자가 필요한 경우엔 여전히 WebClient가 유리하다 — 가상
스레드는 블로킹 코드를 값싸게 만들 뿐, 논블로킹 코드로 바꿔주는 것은 아니기 때문이다.

## 3. POST /api/reservations Rate Limiting

**시나리오**: 인기 지점이 갑자기 풀려서 순간적으로 요청이 몰리는 상황. Redis 토큰 버킷
(`TokenBucketRateLimiter` + `token_bucket.lua`, IP별 버킷, capacity=50/refill=50 per sec)으로
`POST /api/reservations`만 제한한다 — 버킷이 비면 즉시 `429`. "읽고 계산하고 쓰기"는 Lua
스크립트로 Redis 안에서 원자적으로 실행해 동시 요청 간 경합을 막는다.

```bash
APP_RATE_LIMIT_ENABLED=false ./gradlew :reservation-service:bootRun   # 적용 전(비교용)
./gradlew :reservation-service:bootRun                                 # 적용 후(기본값)
BASE_URL=http://localhost:8081 k6 run infra/k6/reservation-spike.js
```

500명이 램프업 없이 거의 동시에 1건씩 요청(스파이크):

| | 적용 전 | 적용 후 |
|---|---|---|
| 성공(201) | 500 / 500 | 92 / 500 |
| 거절(429) | 0 / 500 | 408 / 500 |
| 성공 응답 평균 | 2.09s | 424.57ms |
| 성공 응답 p99 | **2.69s** | **487.87ms** |

적용 전은 500건 전부 성공시켰지만 그 대가로 모든 요청이 함께 느려졌다(p99 2.69초) — 스레드
풀/DB 커넥션이 감당 못 할 트래픽을 억지로 밀어넣어 대기열이 길어진 것이고, 트래픽이 조금만
더 몰렸다면 타임아웃/커넥션 고갈로 전체가 무너질 수 있는 상황이다. 적용 후는 버킷 용량만큼만
즉시 처리하고 나머지는 429로 빠르게 쳐냈다 — 성공한 92건의 응답 시간은 적용 전 평균보다 5배
가까이 빨라졌다. "몰려드는 요청을 전부 받아내려는 것"과 "감당할 수 있는 만큼만 받아 그 안에서는
서비스를 지키는 것" 사이의 트레이드오프다 — 429를 받은 클라이언트가 재시도해도 되는 상황이라면
후자가 시스템 전체를 더 안전하게 지킨다.

## 4. 가용 차량 수 Redis 캐싱

**시나리오**: `GET /api/branches/{branchId}/stock`(차종별 가용 대수)은 예약 폼에서 지점을
고를 때마다 반복 호출된다. Redis에 `availability:{vehicleType}:{branchId}` 키로 cache-aside
캐싱(TTL 10초)했고, 배정/보상으로 실제 재고가 바뀌는 시점(`VehicleAssignmentService`)에
커밋 이후(`afterCommit`)로 해당 키를 무효화한다 — 커밋 전에 지우면 그 사이 다른 요청이
"아직 커밋 전인" 값을 캐시에 다시 채워버리는 race가 있기 때문.

```bash
APP_CACHE_ENABLED=false ./gradlew :vehicle-service:bootRun   # 적용 전(비교용)
./gradlew :vehicle-service:bootRun                            # 적용 후(기본값)
BASE_URL=http://localhost:8082 BRANCH_ID=3014010100 \
  k6 run infra/k6/availability-cache-comparison.js
```

같은 지점을 100 VU가 20초간 반복 조회:

| | 적용 전 | 적용 후 |
|---|---|---|
| TPS | 525.99 req/s | 551.75 req/s |
| p99 | 1.03s | 1.15s |
| **DB 쿼리 수** | **31,695** | **263** |

TPS/p99는 거의 그대로였다 — 이 엔드포인트의 DB 작업이 인덱스 탄 단순 `COUNT` 쿼리라 원래도
싸고, 로컬 환경에서는 HTTP 루프백 왕복이 응답 시간의 대부분을 차지해서 "DB 왕복"이 "Redis
왕복"으로 바뀌어도 체감 차이가 없었다. 대신 DB 쿼리 수는 99.2% 줄었다 — DB가 여러 서비스와
공유되거나 쿼리가 더 무겁거나 인스턴스가 여러 개인 실제 운영 환경이라면, 이 감소가 곧 DB
커넥션 풀 고갈 위험 감소·DB CPU 사용률 감소로 직결된다. "캐싱은 항상 응답 시간을 줄여준다"가
아니라 "병목이 어디인지에 따라 캐싱 효과가 체감되는 지표가 다르다"는 게 이번 실습의 결론이다.
정합성(캐시 히트 시 DB 쿼리 안 늘어남, 배정 직후 stale 값이 아니라 새 값을 즉시 봄)은
`AvailabilityCacheConsistencyTest`로 검증했다.

---

## 네 실습을 관통하는 공통 주제

- **1·2번(블로킹/가상 스레드)** 은 "느린 외부 의존성을 기다리는 동안 내 자원(스레드)을
  어떻게 아낄 것인가"의 문제다 — 논블로킹과 가상 스레드는 서로 다른 메커니즘(이벤트 루프 vs
  캐리어 스레드 반납)으로 같은 목표(적은 OS 스레드로 많은 동시 대기를 처리)를 달성한다.
- **3번(rate limiting)** 은 "내가 감당할 수 없는 트래픽이 들어올 때 그걸 억지로 받아낼
  것인가, 아니면 선별해서 거절할 것인가"의 문제다 — 1·2번이 "더 많이 받아내는" 방향이라면,
  3번은 반대로 "일부러 덜 받는" 선택이다.
  - **선후행 순서 감**: 1·2번으로 처리량 상한을 최대한 끌어올린 뒤에도 여전히 그 상한을
    넘는 스파이크가 올 수 있다 — 3번은 그 남은 초과분을 어떻게 다룰지에 대한 답이다.
- **4번(캐싱)** 은 애초에 DB까지 안 가도 되는 요청을 줄이는 문제다 — 1~3번이 "요청을 어떻게
  처리할지"에 대한 것이라면, 4번은 "처리해야 할 요청 자체를 줄이는" 접근이라 다른 세 가지와
  결합해서 쓸 수 있다(예: 캐시로 걸러진 나머지 요청에 대해 rate limit을 적용).
