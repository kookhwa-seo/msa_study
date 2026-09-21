# 이벤트 스키마 설계

Choreography 기반 Saga에서 서비스 간 주고받는 이벤트 정의. 모든 이벤트는 공통 봉투(envelope) 필드 +
이벤트별 payload 필드로 구성되고, `common` 모듈의 Java record로 정의해 4개 서비스가 동일한 클래스를
직렬화/역직렬화에 사용한다 (Kafka 메시지 값은 JSON).

## 공통 봉투 필드

모든 이벤트가 공통으로 갖는 필드. `DomainEvent` 인터페이스로 강제한다.

| 필드 | 타입 | 설명 |
|---|---|---|
| eventId | UUID | 이벤트 고유 식별자 (멱등 처리에 사용) |
| eventType | String | 이벤트 이름 (예: `ReservationCreated`) |
| occurredAt | Instant | 이벤트 발생 시각 |
| aggregateId | String | Saga 상관관계 ID. 지금 흐름에서는 reservationId로 고정 |
| version | int | 스키마 버전 (하위 호환 깨질 때 증가) |

## 토픽 & 이벤트 목록

| Topic | Event | Producer | Consumer | 용도 |
|---|---|---|---|---|
| `reservation-events` | ReservationCreated | reservation-service | payment-service | 예약 생성 → 가승인 트리거 |
| `payment-events` | PaymentAuthorized | payment-service | vehicle-service | 가승인 성공 → 차량 배정 트리거 |
| `payment-events` | PaymentAuthFailed | payment-service | reservation-service, notification-service | 가승인 거절 → 예약 취소 (배정 전이라 보상 없음) |
| `vehicle-events` | VehicleAssigned | vehicle-service | payment-service, reservation-service | 차량 배정 성공 → 가승인 매입(capture) 트리거 / 예약 PAYMENT_PENDING |
| `vehicle-events` | VehicleAssignFailed | vehicle-service | payment-service | 재고 없음 → 가승인 취소(void) 트리거 |
| `payment-events` | PaymentVoided | payment-service | reservation-service | 가승인 취소(보상) 완료 → 예약 취소 |
| `payment-events` | PaymentCompleted | payment-service | reservation-service, notification-service | 매입 성공 → 예약 확정 |
| `payment-events` | PaymentFailed | payment-service | vehicle-service, notification-service | 매입 실패(가승인 만료 등) → 차량 배정 보상 트리거 |
| `vehicle-events` | VehicleReleased | vehicle-service | reservation-service | 매입 실패로 인한 배정 취소(보상) 완료 |
| `reservation-events` | ReservationCancelled | reservation-service | notification-service | 최종 취소 확정 → 알림 발송 |

결제는 **가승인(authorize) → 차량 배정 → 매입(capture)** 순서다. 가승인은 카드 한도만 보류할 뿐
돈이 움직이지 않으므로, 결제 안 된 주문이 차량을 점유하지 않으면서도 재고 부족 시 보상이 환불이
아니라 보류 해제(void)로 끝난다.

같은 애그리거트(예약)에 대한 이벤트 순서를 보장하기 위해 Kafka 메시지 key는 항상 `reservationId`를
사용한다 (같은 key는 같은 파티션으로 가서 순서가 보장됨).

## 이벤트별 Payload

### ReservationCreated
```json
{
  "eventId": "uuid",
  "eventType": "ReservationCreated",
  "occurredAt": "2026-07-05T10:00:00Z",
  "aggregateId": "reservationId",
  "version": 1,
  "reservationId": "string",
  "customerId": "string",
  "vehicleType": "COMPACT | SUV | VAN",
  "branchId": "string",
  "rentalStartAt": "Instant",
  "rentalEndAt": "Instant",
  "totalAmount": "BigDecimal"
}
```

### PaymentAuthorized
```json
{
  "...envelope",
  "reservationId": "string",
  "paymentId": "string",
  "amount": "BigDecimal",
  "vehicleType": "COMPACT | SUV | VAN",
  "branchId": "string"
}
```
`vehicleType`/`branchId`를 포함하는 이유: 다음 단계인 vehicle-service가 차량을 배정하려면 이 값이
필요하다. vehicle-service가 `reservation-events`를 따로 구독해 로컬에 복사해두면 서로 다른 토픽의
이벤트 도착 순서에 의존하게 되므로(배정이 스냅샷 저장보다 먼저 처리되면 실패), 다음 단계에 필요한
값을 이벤트에 그대로 실어 보내 순서 의존을 없앤다.

### PaymentAuthFailed
```json
{
  "...envelope",
  "reservationId": "string",
  "reason": "CARD_DECLINED | INSUFFICIENT_LIMIT | GATEWAY_TIMEOUT"
}
```
차량 배정 전에 실패하므로 `vehicleId`가 없고, 되돌릴 보상도 없다.

### PaymentVoided
```json
{
  "...envelope",
  "reservationId": "string",
  "paymentId": "string"
}
```

### VehicleAssigned
```json
{
  "...envelope",
  "reservationId": "string",
  "vehicleId": "string",
  "branchId": "string"
}
```

### VehicleAssignFailed
```json
{
  "...envelope",
  "reservationId": "string",
  "reason": "OUT_OF_STOCK | INVALID_VEHICLE_TYPE"
}
```

### PaymentCompleted
```json
{
  "...envelope",
  "reservationId": "string",
  "paymentId": "string",
  "amount": "BigDecimal",
  "paidAt": "Instant"
}
```

### PaymentFailed
```json
{
  "...envelope",
  "reservationId": "string",
  "vehicleId": "string",
  "reason": "CARD_DECLINED | INSUFFICIENT_LIMIT | GATEWAY_TIMEOUT | AUTHORIZATION_EXPIRED"
}
```
매입(capture) 단계에서 실패했을 때만 발행한다 (예: 가승인 유효기간 만료).
`vehicleId`를 포함하는 이유: payment-service는 자기 DB에 vehicleId를 갖고 있지 않지만,
vehicle-service가 보상(재고 반환) 처리할 때 어떤 차량을 되돌려야 하는지 알아야 하기 때문에
Saga 흐름을 타고 온 값을 그대로 실어 보낸다 (Choreography에서 흔한 패턴).

### VehicleReleased
```json
{
  "...envelope",
  "reservationId": "string",
  "vehicleId": "string"
}
```

### ReservationCancelled
```json
{
  "...envelope",
  "reservationId": "string",
  "reason": "OUT_OF_STOCK | PAYMENT_FAILED"
}
```

## Saga 상태 전이 (Reservation 기준)

```
PENDING --(PaymentAuthFailed)--> CANCELLED
PENDING --(PaymentAuthorized → VehicleAssigned)--> PAYMENT_PENDING   # 가승인 + 배정 완료, 매입 대기
PENDING --(PaymentAuthorized → VehicleAssignFailed → payment-service void → PaymentVoided)--> CANCELLED
PAYMENT_PENDING --(VehicleAssigned → 매입 → PaymentCompleted)--> CONFIRMED
PAYMENT_PENDING --(PaymentFailed → vehicle-service 보상 → VehicleReleased)--> CANCELLED
```

`VehicleAssignFailed`와 `PaymentFailed`는 reservation-service가 직접 구독하지 않는다. 각각
payment-service가 가승인을 취소(`PaymentVoided`)하거나 vehicle-service가 배정을 되돌린
(`VehicleReleased`) 뒤에 발행하는 **보상 완료 이벤트**를 받았을 때 비로소 최종 취소(`CANCELLED`)로
전환한다 — Saga 보상은 "실패 이벤트에 직접 반응"하는 게 아니라 "보상이 끝났다는 이벤트에 반응"하는
것이라는 점을 보여주는 지점. `PaymentAuthFailed`만 예외로 직접 취소하는데, 차량 배정 전이라 되돌릴
보상 자체가 없기 때문이다.

## Payment 상태 (payment-service 내부)

```
AUTHORIZED --capture--> CAPTURED    (VehicleAssigned 수신, 실제 청구 확정)
AUTHORIZED --void-----> VOIDED      (VehicleAssignFailed 수신, 보류만 해제)
AUTHORIZED --expire---> EXPIRED     (매입 시점에 가승인 유효기간 경과 → PaymentFailed 발행)
(가승인 시도 거절) --> AUTH_FAILED
```

## 멱등성/재처리 메모
- Consumer는 `eventId` 기준으로 처리 이력을 남겨(처리된 eventId 테이블 또는 유니크 제약) 같은
  이벤트를 중복 컨슘해도 같은 부작용이 두 번 일어나지 않도록 한다. Kafka는 at-least-once
  전달이 기본이라 이 부분을 생략하면 안 된다. (2단계 Outbox 구현 시 상세 설계)
- Outbox 발행 순서와 실제 비즈니스 이벤트 발생 순서가 어긋나지 않도록, 하나의 트랜잭션에서
  발생하는 이벤트는 하나만 outbox에 적재한다 (한 트랜잭션 = 한 이벤트 원칙).
