package com.msastudy.mockpaymentgateway;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 진짜 카드사/PG망은 인터넷을 거쳐 여러 시스템을 통과하기 때문에 응답이 빠르지 않다 - 이
 * 지연을 흉내내려고 요청마다 200~500ms를 일부러 재운다. 이 블로킹 sleep 자체가 "이 서버가
 * 처리하는 데 오래 걸린다"를 뜻하는 게 아니라 "네트워크 너머 응답을 기다리는 시간"을
 * 뜻한다 - 호출하는 쪽(http-client-lab)이 이 대기 시간 동안 자기 스레드를 어떻게 쓰는지가
 * 이번 실습의 핵심이다.
 */
@RestController
@RequestMapping("/api/mock-payment")
public class MockPaymentController {

    private static final int MIN_DELAY_MS = 200;
    private static final int MAX_DELAY_MS = 500;

    @PostMapping("/authorize")
    public Map<String, Object> authorize() throws InterruptedException {
        int delayMs = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS + 1);
        Thread.sleep(delayMs);

        return Map.of(
                "status", "APPROVED",
                "paymentId", UUID.randomUUID().toString(),
                "delayMs", delayMs,
                "processedAt", Instant.now().toString()
        );
    }
}
