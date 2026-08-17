package com.msastudy.llmservice.service;

import com.msastudy.llmservice.llm.Branch;
import com.msastudy.llmservice.llm.BranchCatalog;
import com.msastudy.llmservice.llm.ExtractionResult;
import com.msastudy.llmservice.llm.LlmClient;
import com.msastudy.llmservice.web.ChatRequest;
import com.msastudy.llmservice.web.ChatResponse;
import com.msastudy.llmservice.web.ChatStateDto;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ChatService {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneId.of("Asia/Seoul"));

    private static final Map<String, String> FIELD_LABELS = Map.of(
            "customerId", "고객 ID",
            "vehicleType", "차량 종류(소형/SUV/승합)",
            "model", "차량 모델(쏘나타, 카니발 등)",
            "branchId", "지점(시/구/동 어디든)",
            "rentalStartAt", "대여 시작일",
            "rentalEndAt", "대여 종료일",
            "totalAmount", "결제 예정 금액"
    );

    private static final Map<String, String> VEHICLE_TYPE_LABELS = Map.of(
            "COMPACT", "소형",
            "SUV", "SUV",
            "VAN", "승합차"
    );

    private final LlmClient llmClient;
    private final BranchCatalog branchCatalog;

    public ChatResponse chat(ChatRequest request) {
        ChatStateDto currentState = request.state() != null ? request.state() : ChatStateDto.empty();
        ExtractionResult extraction = llmClient.extract(request.message(), currentState);
        ChatStateDto merged = currentState.mergeWith(extraction);
        List<String> missing = missingFields(merged);
        boolean readyToSubmit = missing.isEmpty();
        String reply = composeReply(extraction, merged, missing, readyToSubmit);
        return new ChatResponse(reply, merged, missing, readyToSubmit);
    }

    private List<String> missingFields(ChatStateDto state) {
        List<String> missing = new ArrayList<>();
        if (state.customerId() == null || state.customerId().isBlank()) {
            missing.add("customerId");
        }
        if (state.vehicleType() == null) {
            missing.add("vehicleType");
        }
        if (state.model() == null || state.model().isBlank()) {
            missing.add("model");
        }
        if (state.branchId() == null) {
            missing.add("branchId");
        }
        if (state.rentalStartAt() == null) {
            missing.add("rentalStartAt");
        }
        if (state.rentalEndAt() == null) {
            missing.add("rentalEndAt");
        }
        if (state.totalAmount() == null) {
            missing.add("totalAmount");
        }
        return missing;
    }

    private String composeReply(ExtractionResult extraction, ChatStateDto merged, List<String> missing, boolean readyToSubmit) {
        StringBuilder reply = new StringBuilder();
        reply.append(extraction.isEmpty()
                ? "죄송해요, 이번 메시지에서는 예약 정보를 찾지 못했어요. "
                : "확인했어요. ");

        if (readyToSubmit) {
            reply.append("필요한 정보를 모두 모았습니다.\n").append(summarize(merged))
                    .append("\n이 내용으로 예약하시려면 '예약 확정하기' 버튼을 눌러주세요.");
        } else {
            reply.append("아직 필요한 정보: ")
                    .append(missing.stream().map(FIELD_LABELS::get).reduce((a, b) -> a + ", " + b).orElse(""))
                    .append(".\n예: \"다음주 화요일부터 2박, 왕십리에서 SUV, 20만원\" 처럼 전국 어느 지역이든 자유롭게 말씀해 주세요.");
        }
        return reply.toString();
    }

    private String summarize(ChatStateDto state) {
        return "고객 ID: %s\n차량 종류: %s (%s)\n지점: %s\n대여 기간: %s ~ %s\n결제 예정 금액: %s원".formatted(
                state.customerId(),
                VEHICLE_TYPE_LABELS.getOrDefault(state.vehicleType(), state.vehicleType()),
                state.model(),
                formatBranch(state.branchId()),
                DATE_FORMAT.format(state.rentalStartAt()),
                DATE_FORMAT.format(state.rentalEndAt()),
                state.totalAmount().toBigInteger()
        );
    }

    private String formatBranch(String branchId) {
        Map<String, Branch> byCode = branchCatalog.getAll().stream()
                .collect(Collectors.toMap(Branch::code, Function.identity()));
        Branch branch = byCode.get(branchId);
        return branch != null ? "%s %s %s".formatted(branch.sido(), branch.sigungu(), branch.dong()) : branchId;
    }
}
