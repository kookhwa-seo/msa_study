package com.msastudy.llmservice.web;

import com.msastudy.llmservice.llm.ExtractionResult;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * 대화가 이어지는 동안 프런트엔드가 들고 있다가 매 요청마다 그대로 되돌려주는 슬롯 상태.
 * 서버는 세션/DB 없이 이 값 + 새 메시지의 추출 결과만으로 다음 상태를 계산한다.
 */
public record ChatStateDto(
        String customerId,
        String vehicleType,
        String model,
        String branchId,
        Instant rentalStartAt,
        Instant rentalEndAt,
        BigDecimal totalAmount
) {

    public static ChatStateDto empty() {
        return new ChatStateDto(null, null, null, null, null, null, null);
    }

    public ChatStateDto mergeWith(ExtractionResult extraction) {
        return new ChatStateDto(
                customerId,
                extraction.vehicleType() != null ? extraction.vehicleType() : vehicleType,
                extraction.model() != null ? extraction.model() : model,
                extraction.branchId() != null ? extraction.branchId() : branchId,
                extraction.rentalStartAt() != null ? extraction.rentalStartAt() : rentalStartAt,
                extraction.rentalEndAt() != null ? extraction.rentalEndAt() : rentalEndAt,
                extraction.totalAmount() != null ? extraction.totalAmount() : totalAmount
        );
    }
}
