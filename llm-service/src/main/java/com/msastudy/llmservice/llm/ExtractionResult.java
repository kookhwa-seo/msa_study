package com.msastudy.llmservice.llm;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * 사용자 메시지 하나에서 새로 찾아낸 필드만 담는다(못 찾은 필드는 null).
 * {@link com.msastudy.llmservice.web.ChatStateDto#mergeWith}가 기존 상태 위에 덮어쓴다.
 */
public record ExtractionResult(
        String vehicleType,
        String model,
        String branchId,
        Instant rentalStartAt,
        Instant rentalEndAt,
        BigDecimal totalAmount
) {

    public boolean isEmpty() {
        return vehicleType == null && model == null && branchId == null && rentalStartAt == null
                && rentalEndAt == null && totalAmount == null;
    }
}
