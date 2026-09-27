# 파티션 수 x 컨슈머 수 처리량 실습

## 왜 하는 실습인가

vehicle-service의 `PaymentAuthorized` 컨슈머(`PaymentEventConsumer` → `VehicleAssignmentService
.handlePaymentAuthorized`)를 대상으로, "파티션을 늘리면 처리량이 느는가"와 "컨슈머를 늘리면
처리량이 느는가"를 직접 재보는 실습. 결론부터 말하면 **파티션은 "최대 몇 명까지 동시에
처리할 수 있는가"의 상한선일 뿐이고, 실제로 그 상한까지 쓰려면 컨슈머(스레드/인스턴스)도
그만큼 있어야 한다**는 걸 숫자로 확인했다.

## 실험 설계

- **대상**: vehicle-service의 실제 `payment-events` 컨슈머 그룹(`vehicle-service`). 배정 로직
  (기존에 구현한 비관적/낙관적 락 비교)은 전혀 건드리지 않았다 - 이 실습은 순수하게 "메시지를
  얼마나 빨리 소비하는가"만 잰다.
- **부하**: `PaymentAuthorizedFloodProducer`(`kafka-lab` 모듈)가 실제 `PaymentAuthorizedEvent`
  스키마 그대로 10,000건을 `payment-events`에 직접 발행한다(reservation-service/payment-service를
  거치지 않고 토픽에 바로 꽂는다 - 안 그러면 payment-service의 outbox 폴링 속도(2초마다 50건)가
  먼저 병목이 돼서 vehicle-service의 처리량을 잴 수가 없다).
- **branchId를 "9999999999"(존재하지 않는 지점)로 고정한 이유**: 이 실습은 배정 성공/실패
  로직 자체를 보려는 게 아니라 순수 소비 처리량만 재는 것이다. 존재하지 않는 지점이면
  vehicle-service는 항상 빠르고 일관되게 "재고 없음"으로 처리하고(잠글 행 자체가 없어서 락
  경합도 없다), 실제 차량 재고를 전혀 건드리지 않는다. 더 중요한 이유: 만약 배정에 "성공"해서
  `VehicleAssigned`가 발행되면 그걸 reservation-service/payment-service가 실제 Saga의 일부로
  착각해서 처리하려다(존재하지 않는 가짜 예약이므로) 예외를 반복하며 poison message backlog를
  만든다 - 동시 예약 방어 실습(`docs/concurrent-booking-lab.md`) 때 실제로 겪은 문제라 이번엔
  처음부터 피해갔다(그래도 실험 동안은 payment-service/reservation-service를 내려두는 이중
  안전장치를 썼다).
- **컨슈머 수 조절**: `PaymentEventConsumer`의 `@KafkaListener(concurrency = "${app.vehicle
  .payment-events-concurrency:1}")`를 프로퍼티로 빼서, `APP_VEHICLE_PAYMENT_EVENTS_CONCURRENCY`
  환경변수로 재기동할 때마다 값을 바꿨다. 이건 같은 JVM 프로세스 안에서 컨슈머 스레드(=Kafka
  프로토콜 관점에서는 별개의 그룹 멤버) 개수를 늘리는 방식이다 - 별도 프로세스를 여러 개 띄우는
  것과 Kafka의 파티션 배정 관점에서는 완전히 동등하다(그룹 멤버가 스레드냐 프로세스냐는
  리밸런싱 프로토콜에 안 보인다). kafka-lab 1번 실습(순서 보장)에서도 같은 원칙을 썼다.
- **측정**: 처음엔 `kafka-consumer-groups.sh`를 셸에서 반복 호출해서 쟀는데, 이 스크립트 자체가
  매번 새 JVM을 띄우는 탓에 호출 하나에 수 초가 걸려 측정값을 신뢰할 수 없었다. 그래서
  `PaymentAuthorizedFloodProducer`가 **발행과 lag 폴링을 전부 같은 프로세스 안에서** Kafka
  AdminClient로 직접 한다(50ms 주기로 committed offset과 log-end-offset의 차이를 계산).

## 실행 방법

```bash
docker compose up -d kafka vehicle-db
# 실험 동안은 poison message 재현을 피하기 위해 payment-service/reservation-service를 내려둔다
./gradlew :vehicle-service:bootRun   # 기본 concurrency=1
# 다른 터미널에서
./gradlew :kafka-lab:floodPaymentAuthorized --args="10000"

# 컨슈머 수를 바꿀 때는 vehicle-service를 재기동
APP_VEHICLE_PAYMENT_EVENTS_CONCURRENCY=2 ./gradlew :vehicle-service:bootRun

# 파티션 수를 바꿀 때 (한 번 늘리면 되돌릴 수 없다 - Kafka 자체 제약)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 \
  --alter --topic payment-events --partitions 4
```

## 실제 실행 결과

| 파티션 수 | 컨슈머 수 | 10,000건 처리 시간 | 처리량 (msg/s) |
|---|---|---|---|
| 1 | 1 | 13,480ms | 741.8 |
| 4 | 1 (파티션 증설 직후, 전환 구간) | 85,833ms | 116.5 |
| 4 | 1 (정상 상태) | 12,829ms | 779.5 |
| 4 | 2 | 6,448ms | 1,550.9 |
| 4 | 4 | 4,594ms | 2,176.8 |
| 4 | 5 (파티션 수 초과) | 4,719ms | 2,119.1 |

### 1) 파티션만 늘리고 컨슈머는 그대로면? → 처리량 그대로

`P1,C1`(741.8 msg/s)과 `P4,C1 정상 상태`(779.5 msg/s)는 오차 범위 안에서 사실상 같다.
컨슈머가 1개뿐이면 파티션이 몇 개든 그 컨슈머 스레드 하나가 전부 순차 처리하기 때문에,
파티션을 늘린 것만으로는 처리량이 늘지 않는다 - 정확히 예상했던 결과.

### 2) (예상 밖의 발견) 파티션을 늘린 직후엔 오히려 확 느려졌다

`P4,C1 전환 구간`이 `85,833ms`로, 정상 상태(`12,829ms`)보다 **6배 이상 느렸다.**
`kafka-topics.sh --alter`로 파티션을 1→4로 늘린 직후, 이미 떠 있던 vehicle-service 컨슈머가
새 파티션 3개의 존재를 바로 알아채지 못했다 - 실제 로그:

```
18:05:52.474  Request joining group due to: cached metadata has changed from
              (version2: {payment-events=[NO_RACKS]}) to (version3: {payment-events=[NO_RACKS x4]})
18:05:52.481  partitions revoked: [payment-events-0]
18:05:52.978  partitions assigned: [payment-events-0, payment-events-1, payment-events-2, payment-events-3]
```

이 리밸런스 자체는 0.5초 만에 끝났지만, **그 리밸런스가 트리거되기까지** 상당히 오래 걸렸다.
그동안 새 파티션 3개(전체 메시지의 약 75%)에 쌓인 메시지는 컨슈머 눈에 아예 안 보였고, 원래
파티션 0(전체의 약 25%)만 느릿느릿 처리되고 있었던 것 - 그래서 리밸런스가 일어나기 전까지는
진행이 거의 멈춘 것처럼 보였다. 리밸런스가 끝나자마자 나머지가 빠르게 처리됐다.

**실무 시사점**: 파티션을 늘리는 건 클러스터 메타데이터 변경이고, 이미 떠 있는 컨슈머가 그
변화를 알아채는 데는 지연이 있을 수 있다. "파티션을 늘렸으니 이제 더 빠르겠지"라고 안심하고
바로 부하를 밀어넣으면, 오히려 일시적으로 더 느려지는 구간을 겪을 수 있다는 걸 직접 확인했다.

### 3) 컨슈머를 늘리면? → 파티션 수까지는 거의 정비례

```
컨슈머 1개(정상 상태): 779.5 msg/s   (기준)
컨슈머 2개           : 1,550.9 msg/s  (약 2.0배)
컨슈머 4개           : 2,176.8 msg/s  (약 2.8배 - 이론상 4배보단 낮음, DB 커넥션 풀 등 공유 자원 경합 추정)
```

### 4) 컨슈머 수가 파티션 수(4개)를 넘으면?

```
컨슈머 4개: 2,176.8 msg/s
컨슈머 5개: 2,119.1 msg/s   (사실상 동일 - 오히려 오차 범위 안에서 약간 낮음)
```

5번째 컨슈머 스레드가 실제로 파티션을 **하나도 못 받은 것**까지 로그로 직접 확인했다:

```
vehicle-service: partitions assigned: [payment-events-1]   <- 1번 스레드
vehicle-service: partitions assigned: [payment-events-3]   <- 2번 스레드
vehicle-service: partitions assigned: [payment-events-0]   <- 3번 스레드
vehicle-service: partitions assigned: [payment-events-2]   <- 4번 스레드
vehicle-service: partitions assigned: []                   <- 5번 스레드, 파티션 0개 - 그냥 논다
```

## 왜 컨슈머 수가 파티션 수를 넘으면 의미가 없는가

Kafka의 컨슈머 그룹은 **"파티션 하나는 같은 그룹 안에서 컨슈머 하나에만 배정한다"**는 규칙으로
동작한다(두 컨슈머가 같은 파티션을 동시에 읽으면 순서 보장이 깨지기 때문 - `docs/kafka-ordering
-lab.md`에서 다룬 "파티션 내부 순서 보장"이 애초에 이 규칙 덕분에 성립한다). 그래서 병렬로
일할 수 있는 컨슈머의 실질적인 수는 **min(컨슈머 수, 파티션 수)**로 정해진다. 파티션이 4개인데
컨슈머가 5개면, 4개는 파티션을 하나씩 맡아 일하고 나머지 1개는 배정받을 파티션이 없어서
그냥 대기만 한다(이번 실습에서 실제로 `assigned: []`로 확인됨) - 그 5번째 컨슈머를 위해 쓴
스레드/커넥션/메모리는 전부 낭비다. 그래서 "컨슈머 수를 늘려서 처리량을 올리고 싶다"는 계획은
반드시 "지금 파티션이 몇 개인가"를 먼저 확인해야 의미가 있다 - 파티션 수가 곧 그 토픽이 낼 수
있는 병렬 처리의 물리적 상한선이다.
