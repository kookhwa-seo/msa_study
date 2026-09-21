package com.msastudy.eureka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

/**
 * 서비스 레지스트리(디스커버리 서버). 이 프로젝트에서 유일하게 동기 REST 호출 경로에 관여하는
 * 인프라 컴포넌트다 - 나머지 서비스 간 통신은 전부 Kafka 이벤트로만 이뤄진다는 원칙은 그대로
 * 유지된다({@code CLAUDE.md} 참고). 이 서버는 "api-gateway가 reservation-service의 여러
 * 인스턴스 중 지금 살아있는 걸 어떻게 찾아서 로드밸런싱할까"라는 문제만 풀어준다.
 */
@SpringBootApplication
@EnableEurekaServer
public class EurekaServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(EurekaServerApplication.class, args);
    }
}
