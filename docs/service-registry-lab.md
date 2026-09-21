# 서비스 레지스트리(Eureka) 실습

## 왜 하는 실습인가

이 프로젝트의 서비스들은 지금까지 전부 Kafka 이벤트로만 통신해서 "서비스가 지금 어디 떠
있는지" 알아낼 필요 자체가 없었다(`CLAUDE.md`의 "서비스 간 직접 REST 호출 금지" 원칙). 하지만
API Gateway는 원래부터 예외다 — 브라우저 요청을 실제 백엔드로 라우팅해주는 진입점 역할이라
동기 HTTP 호출을 한다. 지금까지는 `reservation-service`가 항상 인스턴스 1개, 포트 8081
고정이라 게이트웨이 라우팅 규칙에 주소를 그냥 하드코딩(`uri: http://localhost:8081`)해도
문제가 없었다. 이 실습은 **reservation-service를 인스턴스 2개로 띄운 상태**를 만들어서,
"인스턴스가 여러 개고 그 수가 계속 바뀔 때 게이트웨이가 어떻게 대상을 찾고 분산시키는가"라는
문제를 직접 풀어본다. Saga(reservation/vehicle/payment/notification)는 이 실습과 무관하게
여전히 Kafka로만 통신한다 — 건드린 건 "브라우저 → 게이트웨이 → 백엔드"라는, 원래부터 동기
호출이었던 경로뿐이다.

## 구성

- **`eureka-server`** (신규 모듈, 포트 8761): 레지스트리 서버. `@EnableEurekaServer` 하나면
  끝나는 표준 Spring Cloud Netflix Eureka 서버. 로컬 실습이라 `enable-self-preservation: false`
  (네트워크 파티션 대비 안전장치, 켜두면 등록 해제가 잘 안 일어나 헷갈림) + `eviction-interval
  -timer-in-ms: 5000`(등록 해제 검사 주기를 짧게)로 튜닝했다.
- **`reservation-service`**: `spring-cloud-starter-netflix-eureka-client` 추가, `eureka.client
  .service-url.defaultZone`으로 등록. `eureka.instance.instance-id`에 포트를 포함시켜
  (`reservation-service:8081`, `reservation-service:8091`) 같은 서비스의 여러 인스턴스를
  구분되게 했다. 관측용으로 `InstancePortHeaderFilter`(응답에 `X-Instance-Port` 헤더 추가)를
  추가해서, 실제로 어느 인스턴스가 요청을 처리했는지 `curl -i`로 바로 볼 수 있게 했다.
- **`api-gateway`**: `spring-cloud-starter-netflix-eureka-client` + `spring-cloud-starter-
  loadbalancer` 추가. `reservation-api` 라우트의 `uri`를 `http://localhost:8081`에서
  `lb://reservation-service`로 변경 — `lb://` 스킴은 "고정 주소가 아니라 Eureka에 등록된
  `reservation-service`라는 이름으로 찾아서, 여러 인스턴스가 있으면 로드밸런싱해서 보내라"는
  뜻이다. 나머지 라우트(vehicle/llm/health)는 지금도 인스턴스가 1개뿐이라 그대로 하드코딩
  유지 — 필요한 지점에만 최소한으로 바꿨다.

## 실행 방법

```bash
./gradlew :eureka-server:bootRun &

./gradlew :reservation-service:bootRun &                    # 인스턴스 1: 포트 8081(기본값)
SERVER_PORT=8091 ./gradlew :reservation-service:bootRun &   # 인스턴스 2: 포트 8091

./gradlew :api-gateway:bootRun &

# 등록 확인 (Eureka 대시보드는 브라우저로 http://localhost:8761 도 가능)
curl -s http://localhost:8761/eureka/apps/RESERVATION-SERVICE -H "Accept: application/json"

# 로드밸런싱 확인 - X-Instance-Port가 8081/8091을 번갈아 보여줘야 한다
for i in $(seq 1 10); do curl -s -D - -o /dev/null http://localhost:8085/api/reservations | grep -i x-instance-port; done
```

## 실제 실행 결과

### 1) 등록 — 인스턴스 2개가 각각 등록됨

```json
{"instanceId": "reservation-service:8081", "status": "UP"},
{"instanceId": "reservation-service:8091", "status": "UP"}
```

### 2) 클라이언트 사이드 디스커버리 + 로드밸런싱 — 정확히 번갈아 감

`http://localhost:8085/api/reservations`를 10번 연속 호출한 `X-Instance-Port` 응답 헤더:

```
8091 8081 8091 8081 8091 8081 8091 8081 8091 8081
```

Spring Cloud LoadBalancer의 기본 전략(라운드로빈)대로 두 인스턴스에 정확히 번갈아 분산됐다.
게이트웨이 라우팅 규칙에는 `reservation-service`라는 **이름**만 있을 뿐, 8081/8091이라는 실제
포트는 어디에도 적혀 있지 않다 — 전부 Eureka 조회로 매 순간 알아낸 것이다.

### 3) 헬스체크 기반 등록 해제 + 페일오버 — 예상보다 훨씬 흥미로운 결과

8091 인스턴스를 `kill -9`로 강제 종료(정상 종료 신호를 못 받아 스스로 등록 해제를 못 하는
상황을 재현 — 진짜 장애 상황과 동일)한 뒤 관찰한 실제 타임라인:

```
22:39:13  kill -9로 8091 강제 종료
22:39:22  게이트웨이 호출 결과: 500 / 200(8081) 번갈아 나옴  <- 여전히 죽은 인스턴스로 절반 라우팅
22:39:32.847  [eureka-server 로그] "expired lease for RESERVATION-SERVICE/reservation-service:8091"
              -> Eureka 레지스트리에서는 19초 만에 등록 해제됨 (하트비트 10초 만료 + 5초 검사 주기)
22:40:52  게이트웨이 호출 결과: 여전히 500 / 200 번갈아 나옴  <- 레지스트리는 이미 정리됐는데도!
22:41:24  게이트웨이 호출 결과: 전부 200(8081)  <- 이 시점부터 완전히 정상
```

**Eureka 레지스트리 자체는 19초 만에 죽은 인스턴스를 지웠는데, 게이트웨이가 완전히 죽은
인스턴스를 안 부르게 되기까지는 킬 시점부터 총 약 131초가 걸렸다.** 그 사이 게이트웨이 로그에는
`Connection refused: /192.168.0.209:8091` 예외가 계속 찍혔다 — 레지스트리는 이미 정리됐는데도
그렇다.

**원인**: 게이트웨이는 "매 요청마다 Eureka에 물어보는" 게 아니라, 성능을 위해 여러 겹으로
캐시한다.
1. 게이트웨이 안의 Eureka 클라이언트가 서버의 전체 레지스트리를 주기적으로만 가져온다
   (`eureka.client.registry-fetch-interval-seconds`, 기본 30초).
2. 그 위에 Spring Cloud LoadBalancer가 "이 서비스의 인스턴스 목록"을 또 한 번 캐시한다
   (`spring.cloud.loadbalancer.cache.ttl`, 기본 35초).
3. Eureka 서버 자신도 클라이언트에게 내려주는 레지스트리 응답을 캐시해서 서빙한다
   (`eureka.server.response-cache-update-interval-ms`, 기본 30초).

세 캐시가 겹치면서, "레지스트리에서 실제로 빠졌다"와 "호출하는 쪽이 그 사실을 알게 됐다"
사이에 최대 1~2분 정도 간극이 생길 수 있다는 걸 숫자로 직접 확인했다.

## 결론 / 실무 연결

- 서비스 레지스트리는 "누가 등록/해제됐는지"를 정확히 관리해주지만, **그 사실이 호출하는 쪽에
  실시간으로 전파되는 건 아니다** — 여러 겹의 캐시 때문에 초 단위가 아니라 분 단위 지연이 생길
  수 있다. "레지스트리에서 지웠으니 트래픽이 바로 끊기겠지"라고 가정하면 안 된다.
- 그래서 실무에서는 등록 해제(레지스트리 정리)만으로 안전하다고 보지 않고, 클라이언트 쪽에
  **재시도(retry) + 서킷 브레이커** 같은 보완책을 같이 둔다 — 일부 요청이 죽은 인스턴스로
  잘못 가더라도 연결 실패를 감지해서 즉시 다른 인스턴스로 재시도하면, 캐시가 갱신될 때까지
  기다리지 않고도 사용자에게는 실패가 안 보이게 할 수 있다. 이 실습에는 재시도 필터를 넣지
  않아서 그 캐시 지연이 고스란히 500 에러로 드러났다 — 다음에 이 부분을 보완한다면 가장 먼저
  손댈 지점이다.
- **정상 종료(graceful shutdown)면 이 문제가 훨씬 줄어든다**: 이번엔 일부러 `kill -9`로
  하트비트가 끊기는 "진짜 장애"를 재현했지만, 실제 배포 파이프라인에서 인스턴스를 내릴 때는
  보통 종료 전에 레지스트리에 명시적으로 등록 해제 요청을 보낸다(Eureka 클라이언트는 `SIGTERM`을
  받으면 기본적으로 이걸 자동으로 한다) — 그러면 하트비트 만료를 기다릴 필요 없이 즉시
  레지스트리에서 빠진다. 배포/재시작 같은 "계획된" 종료와 이번처럼 "예기치 않은" 장애의 등록
  해제 속도가 이렇게 다르다는 것도 이번 실습에서 확인한 부분이다.
- 이 프로젝트 규모(로컬, 인스턴스 몇 개)에서는 Eureka 없이 Kubernetes의 `Service`(4번
  오토스케일링 실습에서 이미 써봤다)만으로도 충분하다 — k8s `Service`는 컨트롤 플레인이 파드의
  생사를 직접 감시해서 훨씬 빠르게(초 단위) 엔드포인트 목록을 갱신해준다. Eureka 같은
  애플리케이션 레벨 레지스트리는 k8s가 아닌 환경(일반 VM)이거나, 클라이언트 쪽에서 더 세밀한
  제어(가중치 기반 라우팅, 존 인식 등)가 필요할 때 의미가 커진다.
