package com.msastudy.llmservice.llm;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.common.event.VehicleType;
import com.msastudy.llmservice.web.ChatStateDto;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * 로컬 Ollama(OpenAI 호환 {@code /v1/chat/completions})를 호출하는 실제 LLM 구현체.
 *
 * <p>직접 실험해보니 4B급 로컬 모델은 "다음주 화요일" 같은 상대 날짜 계산에서 반복적으로
 * 틀렸다(날짜 조회표를 프롬프트에 줘도 마찬가지). 그래서 날짜는 이 구현체에서도 여전히
 * 결정적인 {@link KoreanDateExtractor}에 맡긴다. 지점도 같은 이유로 LLM에게 최종 branchId를
 * 직접 고르게 하지 않는다 — 후보가 1,500여 개라 프롬프트에 다 나열할 수도 없다. 대신 LLM은
 * 문장에서 지역을 가리키는 텍스트만 뽑고({@code locationText}), 실제 지점 매칭은
 * {@link BranchMatcher}가 결정적으로 한다.
 */
@Component
@ConditionalOnProperty(prefix = "app.llm", name = "provider", havingValue = "ollama")
@Slf4j
public class OllamaLlmClient implements LlmClient {

    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

    private static final String SYSTEM_PROMPT = """
            당신은 렌터카 예약 챗봇의 정보 추출기입니다. 사용자 문장에서 아래 스키마의 필드만 채워
            JSON 객체 하나만 출력하세요. 마크다운이나 설명 없이 JSON만 출력하세요.

            {"vehicleType": "COMPACT"|"SUV"|"VAN"|null, "model": string|null, "locationText": string|null, "totalAmount": 정수|null}

            규칙:
            - 문장에 언급되지 않은 필드는 null로 두세요. 추측해서 채우지 마세요.
            - 차량 종류: 소형/경차 -> COMPACT, SUV -> SUV, 승합/밴 -> VAN
            - model: 사용자가 구체적인 차량 모델명을 언급하면(예: "쏘나타", "아반떼", "싼타페",
              "카니발") 원문 그대로 추출하세요. 모델명은 언급했지만 차종(vehicleType)은 직접
              말하지 않았다면, 그 모델이 어떤 차종인지 당신이 아는 상식으로 판단해서 vehicleType도
              함께 채우세요 (예: 쏘나타/아반떼 -> COMPACT, 싼타페/투싼/코나 -> SUV, 카니발/스타리아
              -> VAN). 모델을 모르겠으면 vehicleType은 null로 두세요.
            - locationText: 사용자가 언급한 지역을 원문 그대로(가공하지 말고) 추출하세요.
              시/도, 구/군, 동, 랜드마크 등 무엇이든 상관없습니다 (예: "왕십리", "강남구 삼성동",
              "해운대", "서울"). 지역 언급이 전혀 없으면 null로 두세요 — 실제 지점 매칭은 별도
              시스템이 처리하니 여기서는 텍스트만 있는 그대로 뽑으면 됩니다.
            - 금액은 원 단위 정수로 변환하세요 (예: "20만원" -> 200000, "150000원" -> 150000)""";

    private final RestClient restClient;
    private final ObjectMapper objectMapper;
    private final KoreanDateExtractor dateExtractor;
    private final BranchMatcher branchMatcher;
    private final String model;

    public OllamaLlmClient(
            @Value("${app.llm.ollama.base-url}") String baseUrl,
            @Value("${app.llm.ollama.model}") String model,
            @Value("${app.llm.ollama.timeout-seconds}") long timeoutSeconds,
            ObjectMapper objectMapper,
            KoreanDateExtractor dateExtractor,
            BranchMatcher branchMatcher
    ) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(Duration.ofSeconds(5));
        requestFactory.setReadTimeout(Duration.ofSeconds(timeoutSeconds));
        this.restClient = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
        this.objectMapper = objectMapper;
        this.dateExtractor = dateExtractor;
        this.branchMatcher = branchMatcher;
        this.model = model;
    }

    @Override
    public ExtractionResult extract(String message, ChatStateDto currentState) {
        ExtractedFields fields = callOllama(message);

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

        String branchId = branchMatcher.match(fields.locationText()).map(Branch::code).orElse(null);

        return new ExtractionResult(
                normalizeVehicleType(fields.vehicleType()),
                fields.model(),
                branchId,
                rentalStartAt,
                rentalEndAt,
                fields.totalAmount()
        );
    }

    private ExtractedFields callOllama(String message) {
        try {
            OllamaChatResponse response = restClient.post()
                    .uri("/v1/chat/completions")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new OllamaChatRequest(
                            model,
                            List.of(
                                    new OllamaMessage("system", SYSTEM_PROMPT),
                                    new OllamaMessage("user", message)
                            ),
                            new ResponseFormat("json_object")
                    ))
                    .retrieve()
                    .body(OllamaChatResponse.class);

            String content = response.choices().get(0).message().content();
            return objectMapper.readValue(content, ExtractedFields.class);
        } catch (Exception e) {
            log.warn("Ollama 호출/응답 파싱 실패 — 이번 메시지는 차종/모델/지점/금액 추출 없이 진행합니다: {}", e.getMessage());
            return new ExtractedFields(null, null, null, null);
        }
    }

    private String normalizeVehicleType(String value) {
        if (value == null) {
            return null;
        }
        try {
            return VehicleType.valueOf(value.trim().toUpperCase()).name();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private record OllamaMessage(String role, String content) {
    }

    private record ResponseFormat(String type) {
    }

    private record OllamaChatRequest(String model, List<OllamaMessage> messages, ResponseFormat response_format) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record OllamaChoice(OllamaMessage message) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record OllamaChatResponse(List<OllamaChoice> choices) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record ExtractedFields(String vehicleType, String model, String locationText, BigDecimal totalAmount) {
    }
}
