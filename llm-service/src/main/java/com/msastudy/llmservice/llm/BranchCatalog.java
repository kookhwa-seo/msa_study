package com.msastudy.llmservice.llm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

@Component
@Getter
public class BranchCatalog {

    private final List<Branch> all;

    public BranchCatalog(ObjectMapper objectMapper) throws IOException {
        try (var input = new ClassPathResource("branches.json").getInputStream()) {
            this.all = objectMapper.readValue(input, objectMapper.getTypeFactory()
                    .constructCollectionType(List.class, Branch.class));
        }
    }
}
