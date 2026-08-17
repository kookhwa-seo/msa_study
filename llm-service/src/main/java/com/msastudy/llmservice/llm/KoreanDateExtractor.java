package com.msastudy.llmservice.llm;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * 자연어 문장에서 날짜를 뽑아내는 규칙 기반 파서. 실제 LLM이라면 임의의 표현을
 * 이해하겠지만, 이 mock은 흔한 패턴(명시적 날짜, 오늘/내일/모레, "다음주 화요일",
 * "N박" 기간)만 인식한다.
 */
@Component
public class KoreanDateExtractor {

    private static final Pattern ISO_DATE = Pattern.compile("(\\d{4})[.\\-](\\d{1,2})[.\\-](\\d{1,2})");
    private static final Pattern MONTH_DAY = Pattern.compile("(\\d{1,2})월\\s*(\\d{1,2})일");
    private static final Pattern NIGHTS = Pattern.compile("(\\d+)\\s*박");

    private static final Map<String, DayOfWeek> WEEKDAYS = Map.of(
            "월요일", DayOfWeek.MONDAY,
            "화요일", DayOfWeek.TUESDAY,
            "수요일", DayOfWeek.WEDNESDAY,
            "목요일", DayOfWeek.THURSDAY,
            "금요일", DayOfWeek.FRIDAY,
            "토요일", DayOfWeek.SATURDAY,
            "일요일", DayOfWeek.SUNDAY
    );

    public List<LocalDate> extractDates(String message, LocalDate today) {
        List<LocalDate> found = new ArrayList<>();

        Matcher iso = ISO_DATE.matcher(message);
        while (iso.find()) {
            found.add(LocalDate.of(
                    Integer.parseInt(iso.group(1)),
                    Integer.parseInt(iso.group(2)),
                    Integer.parseInt(iso.group(3))
            ));
        }
        if (!found.isEmpty()) {
            return found;
        }

        Matcher monthDay = MONTH_DAY.matcher(message);
        while (monthDay.find()) {
            LocalDate candidate = LocalDate.of(today.getYear(), Integer.parseInt(monthDay.group(1)), Integer.parseInt(monthDay.group(2)));
            found.add(candidate.isBefore(today) ? candidate.plusYears(1) : candidate);
        }
        if (!found.isEmpty()) {
            return found;
        }

        if (message.contains("모레")) {
            found.add(today.plusDays(2));
        } else if (message.contains("내일")) {
            found.add(today.plusDays(1));
        } else if (message.contains("오늘")) {
            found.add(today);
        }
        if (!found.isEmpty()) {
            return found;
        }

        for (Map.Entry<String, DayOfWeek> entry : WEEKDAYS.entrySet()) {
            if (message.contains(entry.getKey())) {
                LocalDate next = nextOccurrenceAfter(today, entry.getValue());
                if (message.contains("다음주") || message.contains("다음 주")) {
                    next = next.plusWeeks(1);
                }
                found.add(next);
                break;
            }
        }

        return found;
    }

    public Optional<Integer> extractNights(String message) {
        Matcher matcher = NIGHTS.matcher(message);
        return matcher.find() ? Optional.of(Integer.parseInt(matcher.group(1))) : Optional.empty();
    }

    private LocalDate nextOccurrenceAfter(LocalDate from, DayOfWeek target) {
        LocalDate date = from.plusDays(1);
        while (date.getDayOfWeek() != target) {
            date = date.plusDays(1);
        }
        return date;
    }
}
