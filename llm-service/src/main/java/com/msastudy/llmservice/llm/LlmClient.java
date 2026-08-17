package com.msastudy.llmservice.llm;

import com.msastudy.llmservice.web.ChatStateDto;

/**
 * 실제 LLM 호출을 감추는 경계. 지금은 {@link MockLlmClient}(규칙 기반)만 있지만,
 * 나중에 Anthropic/OpenAI API를 붙일 때는 이 인터페이스를 구현하는 클래스를
 * 하나 추가하고 {@code app.llm.provider} 설정으로 스위치하면 된다 — 호출부
 * ({@code ChatService})는 바꿀 필요가 없다.
 */
public interface LlmClient {

    ExtractionResult extract(String message, ChatStateDto currentState);
}
