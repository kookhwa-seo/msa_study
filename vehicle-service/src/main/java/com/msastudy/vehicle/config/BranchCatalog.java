package com.msastudy.vehicle.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.msastudy.vehicle.domain.Branch;
import java.io.IOException;
import java.util.List;
import lombok.Getter;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

/**
 * classpath의 {@code branches.json}(1,523개 법정동)을 앱 기동 시 한 번 읽어 메모리에 올려두는
 * 참조 데이터 카탈로그. 자주 바뀌지 않는 정적 데이터라 DB 테이블로 두지 않았다 — 이 클래스가
 * 지점 목록 조회 API와 차량 재고 시딩 양쪽에서 같은 데이터를 제공하는 단일 출처다.
 */
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
