package com.msastudy.mockpaymentgateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 블로킹 vs 논블로킹 비교 실습(docs/blocking-vs-nonblocking-lab.md) 전용 - 실제 카드사/PG망을
 * 흉내 내는 가짜 "외부" 서버. 이 프로젝트의 Saga 참여자와는 무관한 독립 서비스다. DB/Kafka
 * 없이 요청을 받으면 일부러 200~500ms 지연시킨 뒤 승인 결과만 돌려준다.
 */
@SpringBootApplication
public class MockPaymentGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(MockPaymentGatewayApplication.class, args);
    }
}
