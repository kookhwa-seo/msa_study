package com.msastudy.payment.domain;

/**
 * 가승인(authorize) → 매입(capture) 2단계 결제의 상태.
 *
 * <pre>
 * AUTHORIZED --capture--> CAPTURED   (실제 청구 확정)
 * AUTHORIZED --void-----> VOIDED     (보류만 해제, 돈은 움직이지 않음)
 * AUTHORIZED --expire---> EXPIRED    (매입 시점에 승인 유효기간이 지나 있음)
 * (승인 시도 자체가 거절되면 AUTH_FAILED)
 * </pre>
 */
public enum PaymentStatus {
    AUTHORIZED,
    AUTH_FAILED,
    CAPTURED,
    VOIDED,
    EXPIRED
}
