package com.msastudy.payment.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.UniqueKey;
import org.junit.jupiter.api.Test;
import org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy;

/**
 * DB 없이 Hibernate 메타데이터만 만들어 UK 정의를 검사한다. reservation_id UK가 실제 스키마에 반영되는지
 * (컬럼명 오타/네이밍 전략 불일치로 빠지거나 매핑 단계에서 깨지지 않는지)를 지키는 회귀 테스트.
 */
class PaymentSchemaTest {

    @Test
    void paymentHasUniqueConstraintOnReservationId() {
        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySetting(AvailableSettings.DIALECT, PostgreSQLDialect.class.getName())
                // Spring Boot 기본 네이밍 전략과 동일 (reservationId → reservation_id).
                .applySetting(AvailableSettings.PHYSICAL_NAMING_STRATEGY,
                        CamelCaseToUnderscoresNamingStrategy.class.getName())
                .applySetting(AvailableSettings.IMPLICIT_NAMING_STRATEGY, SpringImplicitNamingStrategy.class.getName())
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                .build();
        try {
            Metadata metadata = new MetadataSources(registry).addAnnotatedClass(Payment.class).buildMetadata();

            UniqueKey uk = metadata.getEntityBinding(Payment.class.getName())
                    .getTable().getUniqueKeys().get("uk_payment_reservation_id");

            assertThat(uk).as("uk_payment_reservation_id 제약이 매핑에 존재해야 한다").isNotNull();
            assertThat(uk.getColumns().stream().map(Column::getName).toList()).isEqualTo(List.of("reservation_id"));
        } finally {
            StandardServiceRegistryBuilder.destroy(registry);
        }
    }
}
