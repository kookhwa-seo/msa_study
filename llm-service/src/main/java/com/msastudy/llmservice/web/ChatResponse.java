package com.msastudy.llmservice.web;

import java.util.List;

public record ChatResponse(
        String reply,
        ChatStateDto state,
        List<String> missingFields,
        boolean readyToSubmit
) {
}
