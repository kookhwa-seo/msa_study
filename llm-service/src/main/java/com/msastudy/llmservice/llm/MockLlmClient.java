package com.msastudy.llmservice.llm;

import com.msastudy.common.event.VehicleType;
import com.msastudy.llmservice.web.ChatStateDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 실제 LLM 없이 정규식/키워드로 예약 정보를 추출하는 mock 구현체.
 * "app.llm.provider=mock"일 때 사용된다. 실제 로컬 LLM 연동은
 * {@link OllamaLlmClient}("app.llm.provider=ollama") 참고.
 *
 * <p>지점 인식은 미리 정해둔 몇 개 키워드(강남/서울/해운대/부산)만 알아본다 — "왕십리"처럼
 * 목록에 없는 지명은 인식하지 못한다. 실제 1,500여 개 지점 전체를 자유 텍스트로 알아보려면
 * 지리 지식이 있는 {@link OllamaLlmClient}가 필요하다는 걸 보여주는 지점이기도 하다.
 * 매칭된 키워드를 실제 지점 코드로 바꾸는 건 {@link BranchMatcher}를 그대로 재사용한다.
 */
@Component
@ConditionalOnProperty(prefix = "app.llm", name = "provider", havingValue = "mock", matchIfMissing = true)
@RequiredArgsConstructor
public class MockLlmClient implements LlmClient {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");
    private static final Pattern MAN_WON = Pattern.compile("(\\d+)\\s*만\\s*원?");
    private static final Pattern WON = Pattern.compile("(\\d{4,})\\s*원");

    // 자주 쓰이는 모델 몇 개만 안다 — 목록에 없는 모델은 실제 LLM(OllamaLlmClient)이 필요하다.
    private static final Map<String, VehicleType> KNOWN_MODELS = Map.ofEntries(
            Map.entry("쏘나타", VehicleType.COMPACT),
            Map.entry("아반떼", VehicleType.COMPACT),
            Map.entry("모닝", VehicleType.COMPACT),
            Map.entry("레이", VehicleType.COMPACT),
            Map.entry("싼타페", VehicleType.SUV),
            Map.entry("투싼", VehicleType.SUV),
            Map.entry("코나", VehicleType.SUV),
            Map.entry("쏘렌토", VehicleType.SUV),
            Map.entry("스포티지", VehicleType.SUV),
            Map.entry("카니발", VehicleType.VAN),
            Map.entry("스타리아", VehicleType.VAN)
    );

    private final KoreanDateExtractor dateExtractor;
    private final BranchMatcher branchMatcher;

    @Override
    public ExtractionResult extract(String message, ChatStateDto currentState) {
        String model = extractModel(message);
        VehicleType vehicleType = extractVehicleType(message);
        if (vehicleType == null && model != null) {
            vehicleType = KNOWN_MODELS.get(model);
        }
        String locationText = extractLocationText(message);
        String branchId = branchMatcher.match(locationText).map(Branch::code).orElse(null);
        BigDecimal amount = extractAmount(message);

        List<LocalDate> dates = dateExtractor.extractDates(message, LocalDate.now(ZONE));
        Instant rentalStartAt = null;
        Instant rentalEndAt = null;
        if (!dates.isEmpty()) {
            rentalStartAt = dates.get(0).atStartOfDay(ZONE).toInstant();
            if (dates.size() >= 2) {
                rentalEndAt = dates.get(1).atStartOfDay(ZONE).toInstant();
            } else {
                Optional<Integer> nights = dateExtractor.extractNights(message);
                if (nights.isPresent()) {
                    rentalEndAt = dates.get(0).plusDays(nights.get()).atStartOfDay(ZONE).toInstant();
                }
            }
        }

        return new ExtractionResult(
                vehicleType != null ? vehicleType.name() : null,
                model,
                branchId,
                rentalStartAt,
                rentalEndAt,
                amount
        );
    }

    private String extractModel(String message) {
        return KNOWN_MODELS.keySet().stream()
                .filter(message::contains)
                .findFirst()
                .orElse(null);
    }

    private VehicleType extractVehicleType(String message) {
        String lower = message.toLowerCase();
        if (lower.contains("suv") || message.contains("에스유브이")) {
            return VehicleType.SUV;
        }
        if (lower.contains("van") || message.contains("승합") || message.contains("밴")) {
            return VehicleType.VAN;
        }
        if (lower.contains("compact") || message.contains("소형") || message.contains("경차")) {
            return VehicleType.COMPACT;
        }
        return null;
    }

    private String extractLocationText(String message) {
        if (message.contains("강남")) {
            return "강남";
        }
        if (message.contains("해운대")) {
            return "해운대";
        }
        if (message.contains("서울")) {
            return "서울";
        }
        if (message.contains("부산")) {
            return "부산";
        }
        return null;
    }

    private BigDecimal extractAmount(String message) {
        Matcher manWon = MAN_WON.matcher(message);
        if (manWon.find()) {
            return new BigDecimal(manWon.group(1)).multiply(BigDecimal.valueOf(10_000));
        }
        Matcher won = WON.matcher(message);
        if (won.find()) {
            return new BigDecimal(won.group(1));
        }
        return null;
    }
}
