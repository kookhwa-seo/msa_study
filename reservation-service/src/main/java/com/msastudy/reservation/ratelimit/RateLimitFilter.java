package com.msastudy.reservation.ratelimit;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * POST /api/reservations만 IP당 rate limit을 건다(다른 API는 이 실습 대상이 아니라 그대로
 * 둔다). 순간적으로 트래픽이 몰릴 때(예: 인기 지점 오픈 알림 직후) 서버가 그 트래픽을 전부
 * 받아내려다 죽는 대신, 정해둔 처리 능력만큼만 받고 나머지는 429로 빠르게 돌려보낸다 - "전부
 * 받아내는 것"과 "서비스를 지키는 것" 사이의 트레이드오프를 직접 만들어본다
 * (docs/rate-limiting-lab.md).
 */
@Component
@Slf4j
public class RateLimitFilter extends OncePerRequestFilter {

    private static final String RATE_LIMITED_PATH = "/api/reservations";

    private final TokenBucketRateLimiter rateLimiter;
    private final boolean enabled;

    public RateLimitFilter(
            TokenBucketRateLimiter rateLimiter,
            // 적용 전/후 비교 실습 전용 토글. 평소(true)에는 그냥 켜져 있다.
            @Value("${app.rate-limit.enabled:true}") boolean enabled
    ) {
        this.rateLimiter = rateLimiter;
        this.enabled = enabled;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        boolean targetsRateLimitedEndpoint = enabled
                && "POST".equalsIgnoreCase(request.getMethod())
                && RATE_LIMITED_PATH.equals(request.getRequestURI());

        if (targetsRateLimitedEndpoint) {
            String clientKey = clientIp(request);
            if (!rateLimiter.tryConsume(clientKey)) {
                log.warn("rate limit 초과, 429 응답: clientIp={}", clientKey);
                response.setStatus(429); // Too Many Requests - jakarta.servlet.HttpServletResponse에 상수가 없다
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                response.getWriter().write("{\"error\":\"TOO_MANY_REQUESTS\",\"message\":\"요청이 너무 많습니다. 잠시 후 다시 시도해주세요.\"}");
                return;
            }
        }

        chain.doFilter(request, response);
    }

    /**
     * 로컬 실습 환경이라 프록시(X-Forwarded-For) 뒤에 있는 상황은 고려하지 않는다 - 실제
     * 운영에서 로드밸런서/게이트웨이 뒤라면 그 헤더를 신뢰할 수 있는 범위에서 우선 써야 한다.
     */
    private String clientIp(HttpServletRequest request) {
        return request.getRemoteAddr();
    }
}
