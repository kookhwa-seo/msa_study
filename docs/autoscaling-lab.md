# 오토스케일링(HPA) 실습

## 왜 하는 실습인가

지금까지는 오토스케일링이 설정돼 있는 걸 본 적은 있어도 직접 처음부터 구성해본 적은 없었다.
그래서 이번엔 **로컬 k8s 클러스터를 직접 만들고, 이 프로젝트의 실제 서비스를 컨테이너로 빌드해서
배포하고, HPA(HorizontalPodAutoscaler)를 직접 설정하고, k6로 부하를 줘서 실제로 스케일 아웃/인이
되는 전체 과정을 처음부터 끝까지 손으로 해본다.

## 왜 로컬 k8s(kind)인가

AWS ECS 같은 실제 클라우드 오토스케일링도 검토했지만, 이 프로젝트는 학습용이라 비용이 들고
AWS 계정/자격 증명 설정이 필요한 방식보다 **완전히 로컬에서 재현 가능한 방식**을 택했다
(사용자가 명시적으로 선택). `kind`(Kubernetes IN Docker)는 Docker 컨테이너 하나를 k8s 노드처럼
동작시켜주는 도구라 별도 VM 없이 몇 초 만에 클러스터를 만들고 지울 수 있다.

## 구성

- **대상 서비스**: `reservation-service` (이 프로젝트의 메인 비즈니스 REST API, `GET
  /api/reservations`). vehicle-service/payment-service는 이번 실습 범위 밖.
- **DB/Kafka**: 새로 안 띄우고 이미 `docker-compose`로 떠 있는 `reservation-db`/`kafka`를
  그대로 쓴다. kind 클러스터의 노드는 결국 Docker 컨테이너라, Docker Desktop이 제공하는
  `host.docker.internal` DNS로 호스트의 Postgres(5432)/Kafka(9092)에 그대로 접근할 수 있었다
  (직접 `nc -zv host.docker.internal 5432/9092`로 먼저 확인).
- **이미지**: `reservation-service/Dockerfile` — 호스트에서 미리 만든 bootJar를 그대로 복사만
  하는 최소 이미지 (`eclipse-temurin:21-jre-alpine`). 이미지 안에서 Gradle을 다시 돌리지 않아
  빌드가 훨씬 빠르다.
- **매니페스트**: `infra/k8s/reservation-service/manifests.yaml` — Deployment(파드 실행 방법 +
  리소스 request/limit) / Service(고정 접근 주소) / HorizontalPodAutoscaler(스케일링 규칙) 세
  개를 한 파일에 정의했다.
  - CPU request를 `200m`으로 낮게 잡았다 — request가 낮을수록 같은 절대 CPU 사용량이라도
    사용률(%)이 커져서, 적당한 부하로도 HPA 목표치를 넘기는 걸 확인하기 쉽다 (idle 상태에서도
    JVM 자체가 65m 정도를 쓰고 있었다 — request 대비 32%).
  - HPA 목표: CPU 사용률 **50%**, 최소 1개 ~ 최대 5개 파드.
  - `behavior.scaleDown.stabilizationWindowSeconds`를 기본값(5분)에서 30초로 줄였다 — 로컬
    실습을 한 번 돌리는 데 5분씩 기다리긴 너무 길어서. **운영 환경에서는 기본값(5분)이 더
    안전하다** — 너무 짧으면 부하가 잠깐 튀었다 가라앉을 때마다 파드를 늘렸다 줄였다 반복하는
    "플래핑(flapping)"이 생길 수 있다. 이 값은 순전히 데모 편의를 위한 튜닝이라고 매니페스트에도
    주석으로 남겨뒀다.
- **부하 테스트**: `infra/k8s/reservation-service/load-test.js` (k6) — `GET /api/reservations`를
  0→40 VU로 30초간 램프업, 40 VU로 2분 유지, 30초간 0으로 램프다운. `kubectl port-forward`로
  로컬 18081 포트를 통해 접근했다.

## 실행 방법

```bash
brew install kind k6
kind create cluster --name msa-study
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl patch deployment metrics-server -n kube-system --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
  # kind 노드는 kubelet 인증서가 metrics-server 기본 검증을 통과하지 못해서 이 옵션이 필요하다

./gradlew :reservation-service:bootJar
docker build -t reservation-service:hpa-lab -f reservation-service/Dockerfile reservation-service
kind load docker-image reservation-service:hpa-lab --name msa-study

docker compose up -d reservation-db kafka
kubectl apply -f infra/k8s/reservation-service/manifests.yaml

kubectl port-forward svc/reservation-service 18081:8081 &
k6 run infra/k8s/reservation-service/load-test.js

# 다른 터미널에서 실시간 관찰
kubectl get hpa reservation-service -w
kubectl get pods -l app=reservation-service -w
```

## 실제 실행 결과

k6가 실제로 56,415건의 요청을 보냈고 실패율 0%, 평균 응답시간 6.28ms, 처리량 313 req/s였다
(요청 자체는 계속 정상 처리됐다 — HPA는 "느려져서 늘리는" 게 아니라 "CPU를 많이 쓰니까 늘리는"
것임을 보여준다). `kubectl get hpa`를 5초 간격으로 기록한 실제 타임라인:

```
17:27:26  cpu:  20%/50%   pods=1     <- k6 시작 전, 평온한 상태
   ... (부하 시작, 40 VU까지 램프업) ...
17:28:12  cpu: 228%/50%   pods=3     <- CPU가 목표(50%)를 크게 넘자 즉시 스케일 아웃 시작
17:28:27  cpu: 253%/50%   pods=5     <- 최대치(5개)까지 약 20초 만에 도달
17:28:38  cpu: 147%/50%   pods=5     <- 파드가 늘어나면서 파드당 부하 분산 -> 사용률 하락
17:29:08  cpu:  49%/50%   pods=5     <- 목표치 근처로 안정화 (40 VU 부하를 5개가 나눠 처리)
   ... (k6가 40 VU를 2분 유지하다가 30초에 걸쳐 0으로 램프다운, 3분 시점 종료) ...
17:29:54  cpu:  42%/50%   pods=5->4  <- 부하가 빠지자 스케일 인 시작
17:30:10  cpu:  41%/50%   pods=4
17:31:11  cpu:  15%/50%   pods=4->3
17:31:26  cpu:  12%/50%   pods=3->2
17:31:41  cpu:  10%/50%   pods=2->1
17:32:07  cpu:  10%/50%   pods=1     <- 완전히 원래대로 복귀
17:35:57  cpu:   9%/50%   pods=1     <- 이후 계속 안정적으로 1개 유지 확인
```

**스케일 아웃은 빠르고(1→5개, 약 20초), 스케일 인은 훨씬 느리다(5→1개, 약 2분 13초).** 이건
우연이 아니라 매니페스트에서 의도적으로 다르게 설정한 정책 때문이다: 스케일 아웃은
`stabilizationWindowSeconds: 0` + 15초마다 최대 2개씩 늘리기(위험 신호에는 빠르게 반응), 스케일
인은 `stabilizationWindowSeconds: 30` + 15초마다 1개씩만 줄이기(부하가 잠깐 주춤한 걸 착각해서
너무 빨리 줄였다가 다시 부하가 몰리는 플래핑을 피하기 위해 신중하게). "늘릴 땐 빠르게, 줄일 땐
신중하게"가 대부분의 HPA 운영에서 쓰는 기본 철학이다.

## 결론 / 실무 연결

- HPA는 "요청이 실패하거나 느려지는 걸 보고" 반응하는 게 아니라, **파드 리소스 사용률(여기서는
  CPU)이라는 지표를 보고** 반응한다. 그래서 지표를 뭘로 잡느냐(CPU vs 메모리 vs 커스텀 지표),
  목표치를 몇 %로 잡느냐가 오토스케일링 설계의 핵심이다.
- resource **request**가 스케일링 기준의 분모라는 게 처음엔 직관적이지 않았다 — 실제 CPU
  사용량이 똑같아도 request를 낮게 잡으면 사용률(%)이 커져서 더 쉽게(더 예민하게) 스케일
  아웃된다. request를 잘못 잡으면(너무 낮음: 과민 반응, 너무 높음: 정작 필요할 때 반응 없음)
  오토스케일링 자체가 무의미해질 수 있다.
- 스케일 아웃/인에 각각 다른 속도로 반응하게 만드는 `behavior` 설정이 실무에서 중요한 이유를
  직접 시간을 재보고(20초 vs 2분 13초) 체감했다 — 프로덕션 트래픽처럼 "순간적으로 튀었다 바로
  가라앉는" 패턴에서 스케일 인을 너무 빠르게 하면, 파드를 줄이자마자 다시 트래픽이 몰려서
  또 늘려야 하는 악순환(플래핑)이 생긴다.
- kind로 만든 로컬 클러스터도 실제 쿠버네티스 API(Deployment/Service/HPA, metrics-server)를
  그대로 쓰기 때문에, 여기서 익힌 개념과 `kubectl` 조작은 실제 클라우드 k8s(EKS/GKE 등)에서도
  거의 그대로 통한다 — 차이는 노드가 진짜 VM이냐 Docker 컨테이너냐 정도다.
