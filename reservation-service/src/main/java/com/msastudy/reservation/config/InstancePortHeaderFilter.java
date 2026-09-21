package com.msastudy.reservation.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * 서비스 레지스트리/로드밸런싱 실습 전용 관측 도구. 응답 헤더에 이 인스턴스가 실제로 듣고 있는
 * 포트를 실어 보내서, api-gateway가 reservation-service의 여러 인스턴스 중 실제로 어느 쪽으로
 * 요청을 분산시켰는지 {@code curl -i}로 바로 확인할 수 있게 한다. 비즈니스 로직과 무관.
 */
@Component
public class InstancePortHeaderFilter extends OncePerRequestFilter {

    private final String port;

    public InstancePortHeaderFilter(@Value("${server.port}") String port) {
        this.port = port;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        response.setHeader("X-Instance-Port", port);
        chain.doFilter(request, response);
    }
}
