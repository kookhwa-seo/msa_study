package com.msastudy.llmservice.llm;

/**
 * vehicle-service의 {@code Branch}와 동일한 모양. 두 서비스가 서로 REST로 호출하지 않고도
 * 같은 지점 데이터를 알 수 있도록 {@code branches.json}을 각자 번들에 복사해 뒀다
 * (서비스 독립 실행 원칙 — {@link BranchCatalog} 참고).
 */
public record Branch(String code, String sido, String sigungu, String dong) {
}
