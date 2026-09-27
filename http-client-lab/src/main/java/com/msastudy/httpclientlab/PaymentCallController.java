package com.msastudy.httpclientlab;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * 같은 가짜 외부 결제망(mock-payment-gateway, 200~500ms 지연)을 두 가지 방식으로 부른다.
 *
 * <ul>
 *   <li>{@code /blocking}: {@link RestClient}로 동기 호출한다. 응답이 올 때까지 이 요청을 맡은
 *       Tomcat 스레드가 통째로 묶인다 - 외부 서버가 느릴수록 그 스레드는 아무 일도 못 하고
 *       대기만 한다.</li>
 *   <li>{@code /nonblocking}: {@link WebClient}로 비동기 호출하고 {@link Mono}를 그대로
 *       반환한다. Spring MVC가 리액티브 반환 타입을 서블릿 3.1 비동기 처리(DeferredResult)로
 *       자동 변환해주기 때문에, 응답을 기다리는 동안 Tomcat 스레드는 풀로 반납되고 다른 요청을
 *       받을 수 있다 - 외부 응답이 도착하면(reactor-netty 이벤트 루프 스레드가) 그제서야 다시
     *   Tomcat 스레드를 빌려서 응답을 완성한다. 같은 Tomcat(플랫폼 스레드) 위에서 동작한다는
     *   전제는 두 엔드포인트 모두 동일하다 - 차이는 "그 스레드를 대기 중에 붙잡고 있느냐"뿐이다.</li>
 * </ul>
 */
@RestController
@RequiredArgsConstructor
public class PaymentCallController {

    private final RestClient restClient;
    private final WebClient webClient;

    @PostMapping("/blocking")
    public String blocking() {
        return restClient.post()
                .uri("/api/mock-payment/authorize")
                .retrieve()
                .body(String.class);
    }

    @PostMapping("/nonblocking")
    public Mono<String> nonblocking() {
        return webClient.post()
                .uri("/api/mock-payment/authorize")
                .retrieve()
                .bodyToMono(String.class);
    }
}
