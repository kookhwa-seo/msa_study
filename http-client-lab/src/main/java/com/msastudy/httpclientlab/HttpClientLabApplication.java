package com.msastudy.httpclientlab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 블로킹(RestClient) vs 논블로킹(WebClient) vs 가상 스레드 비교 실습
 * (docs/blocking-vs-nonblocking-lab.md, docs/virtual-thread-http-lab.md) 전용 앱. 이 프로젝트의
 * Saga와는 무관한 독립 서비스 - mock-payment-gateway(가짜 외부 결제망)를 호출하는 방식만 바꿔가며
 * 비교한다.
 */
@SpringBootApplication
public class HttpClientLabApplication {

    public static void main(String[] args) {
        SpringApplication.run(HttpClientLabApplication.class, args);
    }
}
