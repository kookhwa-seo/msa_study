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
| `reservation-events` | ReservationCreated | reservation-service | vehicle-service | 예약 생성 → 차량 배정 트리거 |
| `vehicle-events` | VehicleAssigned | vehicle-service | payment-service | 차량 배정 성공 → 결제 트리거 |
| `vehicle-events` | VehicleAssignFailed | vehicle-service | reservation-service | 재고 없음 → 예약 취소 |
| `vehicle-events` | VehicleReleased | vehicle-service | reservation-service, notification-service | 결제 실패로 인한 배정 취소(보상) 완료 |
| `payment-events` | PaymentCompleted | payment-service | reservation-service, notification-service | 결제 성공 → 예약 확정 |
| `payment-events` | PaymentFailed | payment-service | vehicle-service, notification-service | 결제 실패 → 차량 배정 보상 트리거 |
| `reservation-events` | ReservationCancelled | reservation-service | notification-service | 최종 취소 확정 → 알림 발송 |

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
  "reason": "CARD_DECLINED | INSUFFICIENT_LIMIT | GATEWAY_TIMEOUT"
}
```
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
PENDING --(VehicleAssignFailed)--> CANCELLED
PENDING --(VehicleAssigned)--> PAYMENT_PENDING
PAYMENT_PENDING --(PaymentCompleted)--> CONFIRMED
PAYMENT_PENDING --(PaymentFailed → vehicle-service 보상 → VehicleReleased)--> CANCELLED
```

`PaymentFailed`는 reservation-service가 직접 구독하지 않는다. vehicle-service가 이를 구독해
배정했던 차량을 재고로 되돌리는 보상 트랜잭션을 수행한 뒤 `VehicleReleased`를 발행하고,
reservation-service는 그 이벤트를 받았을 때 비로소 최종 취소(`CANCELLED`)로 전환한다 — Saga
보상은 "실패 이벤트를 직접 반응"하는 게 아니라 "보상이 끝났다는 이벤트에 반응"하는 것이라는
점을 보여주는 지점.

## 멱등성/재처리 메모
- Consumer는 `eventId` 기준으로 처리 이력을 남겨(처리된 eventId 테이블 또는 유니크 제약) 같은
  이벤트를 중복 컨슘해도 같은 부작용이 두 번 일어나지 않도록 한다. Kafka는 at-least-once
  전달이 기본이라 이 부분을 생략하면 안 된다. (2단계 Outbox 구현 시 상세 설계)
- Outbox 발행 순서와 실제 비즈니스 이벤트 발생 순서가 어긋나지 않도록, 하나의 트랜잭션에서
  발생하는 이벤트는 하나만 outbox에 적재한다 (한 트랜잭션 = 한 이벤트 원칙).
