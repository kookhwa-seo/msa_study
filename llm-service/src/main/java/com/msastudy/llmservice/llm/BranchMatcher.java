package com.msastudy.llmservice.llm;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 자유 텍스트로 언급된 지역("왕십리", "강남구 삼성동", "해운대")을 실제 지점(법정동) 하나로
 * 매핑하는 결정적 매칭기. LLM/mock 둘 다 지역 "텍스트"만 추출하고, 그 텍스트를 실제
 * 1,500여 개 지점 카탈로그와 맞춰보는 건 이 클래스가 한다 — 날짜 계산을
 * {@code KoreanDateExtractor}에 맡긴 것과 같은 이유다(LLM에게 통째로 맡기기엔 후보가
 * 너무 많고, 정확한 매칭은 코드가 더 안정적이다).
 *
 * <p>동 이름(예: "삼성동")까지 정확히 일치하면 그 지점을 쓰고, 구/군 이름까지만 나오면
 * (예: "강남구", "해운대") 그 구의 첫 동을, 시/도까지만 나오면(예: "서울", "부산") 그
 * 도시의 첫 동을 대표 지점으로 사용한다 — 실제 강남구/해운대구엔 그 이름 그대로인 동이
 * 없어서(강남구는 역삼동·삼성동 등으로 구성) 이런 단계적 완화가 필요하다.
 */
@Component
@RequiredArgsConstructor
public class BranchMatcher {

    private static final int MIN_TEXT_LENGTH = 2;
    private static final Comparator<Branch> BY_CODE = Comparator.comparing(Branch::code);

    private final BranchCatalog branchCatalog;

    public Optional<Branch> match(String rawText) {
        if (rawText == null || rawText.isBlank()) {
            return Optional.empty();
        }
        // "강남구 삼성동"처럼 여러 단어가 붙어 오면 단어 단위로 쪼개서 각각 시도한다 —
        // 동 이름은 대개 한 단어라 전체 문구를 통째로 대조하면 매칭에 실패한다.
        List<String> tokens = Stream.of(rawText.trim().split("\\s+"))
                .filter(t -> t.length() >= MIN_TEXT_LENGTH)
                .toList();
        if (tokens.isEmpty()) {
            return Optional.empty();
        }

        List<Branch> all = branchCatalog.getAll();

        for (String text : tokens) {
            Optional<Branch> byDong = all.stream()
                    .filter(b -> b.dong().contains(text))
                    .min(BY_CODE);
            if (byDong.isPresent()) {
                return byDong;
            }
        }

        for (String text : tokens) {
            Optional<String> matchedSigungu = all.stream()
                    .map(Branch::sigungu)
                    .distinct()
                    .filter(sigungu -> sigungu.contains(text))
                    .findFirst();
            if (matchedSigungu.isPresent()) {
                return all.stream()
                        .filter(b -> b.sigungu().equals(matchedSigungu.get()))
                        .min(BY_CODE);
            }
        }

        for (String text : tokens) {
            Optional<String> matchedSido = all.stream()
                    .map(Branch::sido)
                    .distinct()
                    .filter(sido -> sido.contains(text))
                    .findFirst();
            if (matchedSido.isPresent()) {
                return all.stream()
                        .filter(b -> b.sido().equals(matchedSido.get()))
                        .min(BY_CODE);
            }
        }

        return Optional.empty();
    }
}
