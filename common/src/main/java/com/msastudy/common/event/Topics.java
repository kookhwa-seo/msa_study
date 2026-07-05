package com.msastudy.common.event;

/**
 * Kafka topic 이름 계약. 프로듀서/컨슈머가 서로 다른 서비스에 있어도 같은 상수를
 * 참조하도록 common에 둔다. 메시지 key는 항상 reservationId(Saga 상관관계 ID).
 */
public final class Topics {

    public static final String RESERVATION_EVENTS = "reservation-events";
    public static final String VEHICLE_EVENTS = "vehicle-events";
    public static final String PAYMENT_EVENTS = "payment-events";

    private Topics() {
    }
}
