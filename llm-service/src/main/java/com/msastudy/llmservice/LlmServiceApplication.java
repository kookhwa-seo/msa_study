package com.msastudy.llmservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 자연어 예약 어시스턴트. DB/Kafka 없이 REST 요청 하나로 슬롯 채우기(slot filling)를
 * 수행하는 무상태 서비스다. {@link com.msastudy.llmservice.llm.LlmClient} 구현체를
 * 규칙 기반 {@link com.msastudy.llmservice.llm.MockLlmClient}에서 실제 Claude/OpenAI
 * API 호출로 교체하면 실제 LLM 연동으로 전환할 수 있다.
 */
@SpringBootApplication
public class LlmServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LlmServiceApplication.class, args);
    }
}
