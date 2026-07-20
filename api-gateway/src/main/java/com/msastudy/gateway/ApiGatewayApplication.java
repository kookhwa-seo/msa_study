package com.msastudy.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 클라이언트가 각 서비스 포트(8081~8084)에 직접 붙는 대신, 이 게이트웨이 하나만
 * 알면 되게 하는 단일 진입점. 다른 서비스들과 달리 자체 도메인 로직이 없어
 * common 모듈에 의존하지 않는다 — 순수 라우팅 계층.
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
