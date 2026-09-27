# 가용 차량 수 Redis 캐싱 실습

## 배경

`GET /api/branches/{branchId}/stock`(차종별 가용 대수 조회)은 예약 폼에서 사용자가 지점을
고를 때마다, 심지어 그냥 화면을 훑어보기만 해도 반복 호출된다. 인기 지점이라면 짧은
시간에 같은 지점의 같은 조회가 수십~수백 번 겹칠 수 있는데, 매번 DB에 `COUNT` 쿼리를
3번(차종 수만큼) 던지는 건 낭비다 - 같은 지점의 재고는 배정/보상이 일어나지 않는 한
바뀌지 않으므로, 짧은 시간 동안은 방금 계산한 값을 그대로 재사용해도 된다.

## 왜 JPA 영속성 컨텍스트로는 부족한가

JPA의 1차 캐시(영속성 컨텍스트)는 트랜잭션(요청) 하나 안에서만 유효하고, 그 트랜잭션이
끝나면 사라진다. 즉 "같은 요청 안에서 같은 엔티티를 두 번 조회하면 캐싱된다"는 보장이지,
"수백 명의 서로 다른 요청이 같은 값을 공유해서 본다"는 보장이 아니다. 여러 요청·여러
인스턴스가 값을 공유해서 봐야 하는 이번 케이스엔 애초에 범위가 안 맞는다. Redis 캐시는
반대로 요청/트랜잭션 경계와 무관하게, TTL이 끝나거나 명시적으로 지우기 전까지 모든 요청이
같은 값을 공유해서 본다 - 그래서 "누가 캐시를 무효화해야 하는가"를 직접 챙겨야 하는
책임이 함께 따라온다.

## 구성

- `AvailabilityCacheService` - `availability:{vehicleType}:{branchId}` 키로 Redis에
  카운트를 저장(TTL 10초). `BranchController.stock()`이 cache-aside 패턴으로 사용한다:
  캐시에 있으면 그대로 반환, 없으면 DB `COUNT` 쿼리 후 캐시에 채운다.
- 무효화: `VehicleAssignmentService`의 프로덕션 배정 경로(`handlePaymentAuthorized`)와
  보상 경로(`handlePaymentFailed`)에서 재고가 실제로 바뀔 때 해당 차종+지점 키를 지운다.
  **커밋 이후에** 지우도록 `TransactionSynchronizationManager.registerSynchronization`
  (`afterCommit`)으로 미뤘다 - 커밋 전에 바로 지우면, 그 사이(evict 직후 ~ 실제 commit
  전)에 다른 요청이 재고를 조회해서 "아직 커밋 전이라 예전 그대로인" 값을 캐시에 다시
  채워버릴 수 있고, 그러면 우리 트랜잭션이 실제로 커밋된 뒤에도 캐시엔 evict가 무의미해진
  stale 값이 다음 TTL 만료까지 남는다.
- TTL(10초)은 무효화를 놓치는 경우(코드 버그 등)에 대한 안전판이지, 정상 경로에서
  의존하는 값은 아니다 - 정상적으로는 배정/보상 즉시 무효화되므로 TTL이 끝나기 전에
  이미 최신 값이 다시 캐싱된다.
- `app.cache.enabled`(기본 true) 토글로 적용 전/후를 비교할 수 있다. 꺼도 항상 DB를
  직접 조회할 뿐 오류 없이 정상 동작한다.
- `DbQueryCounter` + `GET/POST /api/branches/debug/db-query-count`(reset) - 캐싱 전/후
  DB 쿼리 수를 실측 비교하기 위한 실습 전용 디버그 엔드포인트.

## 정합성 검증

`AvailabilityCacheConsistencyTest`(`@Tag("integration")`, 실제 vehicle-db + Redis 필요):
1. 같은 지점을 두 번 조회하면 두 번째는 캐시 히트라 DB 쿼리 카운터가 늘지 않는지 확인.
2. 차량을 실제로 배정(`handlePaymentAuthorized`)한 뒤, 캐시가 즉시(TTL을 기다리지 않고)
   무효화됐는지, 그리고 다음 조회가 캐시에 남아있던 stale 값(1)이 아니라 배정으로 줄어든
   새 값(0)을 반환하는지 확인.

```bash
docker compose up -d vehicle-db redis
./gradlew :vehicle-service:concurrencyTest --tests "com.msastudy.vehicle.cache.AvailabilityCacheConsistencyTest"
```

두 테스트 모두 통과 확인함(2/2, 0 failures).

## 실행 방법

```bash
# 적용 전(비교용)
APP_CACHE_ENABLED=false ./gradlew :vehicle-service:bootRun

# 적용 후(기본값)
./gradlew :vehicle-service:bootRun

BASE_URL=http://localhost:8082 BRANCH_ID=3014010100 \
  k6 run infra/k6/availability-cache-comparison.js
```

`infra/k6/availability-cache-comparison.js`는 재고가 있는 실제 지점(`3014010100`, 시딩
데이터 기준 VAN 3대) 하나를 100 VU가 20초 동안 반복 조회한다(인기 지점 상세 페이지에
여러 사용자가 몰리는 상황).

## 실제 실행 결과

| | 적용 전(매 요청마다 DB 조회) | 적용 후(Redis 캐시, TTL 10초) |
|---|---|---|
| 요청 수(20초) | 10,565 | 11,172 |
| TPS | 525.99 req/s | 551.75 req/s |
| p99 | 1.03s | 1.15s |
| **DB 쿼리 수** | **31,695** | **263** |

## 해석: TPS/p99는 거의 그대로인데 DB 쿼리 수는 왜 99% 넘게 줄었나

예상과 다르게 TPS와 p99는 적용 전/후로 거의 차이가 없다(오히려 적용 후 p99가 근소하게
더 높다 - 측정 노이즈 범위). 이유는 이 엔드포인트가 하는 DB 작업이 로컬 Postgres에서
인덱스 탄 단순 `COUNT` 쿼리 3번뿐이라 원래도 매우 싸기 때문이다 - 이번 부하 테스트에서는
DB 쿼리 비용이 아니라 로컬 루프백 네트워크 왕복(HTTP 요청/응답)이 전체 응답 시간의
대부분을 차지했고, 캐시를 껴도 "DB 왕복"이 "Redis 왕복"으로 바뀔 뿐이라 클라이언트가
체감하는 시간은 크게 달라지지 않았다.

반면 **DB 쿼리 수는 31,695 -> 263으로 99.2% 줄었다** - 이게 이번 캐싱의 실질적인 효과다.
지금은 로컬 단일 인스턴스 + 전용 DB라 이 감소가 응답 시간으로 체감되지 않지만, 실제
운영 환경이라면 이야기가 다르다: DB가 여러 서비스와 공유되거나 네트워크 너머(RDS 등)에
있어서 쿼리 자체의 왕복 비용이 크거나, 쿼리가 `COUNT`보다 무겁거나(조인/집계), 여러
서비스 인스턴스가 같은 DB에 동시에 부하를 준다면, DB 쿼리 수 99% 감소가 곧 DB 커넥션
풀 고갈 위험 감소, DB CPU 사용률 감소, 다른 쿼리들의 대기 시간 감소로 직결된다. 즉
"캐싱이 항상 응답 시간을 극적으로 줄여준다"가 아니라, "병목이 어디 있는지에 따라 캐싱의
효과가 체감되는 지표가 다르다"는 게 이번 실습에서 실측으로 확인한 결론이다.
