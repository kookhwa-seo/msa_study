package com.msastudy.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Kafka 리스너 장애 격리 설정. 메시지 처리가 실패하면 지수 백오프로 재시도하고,
 * 재시도를 다 소진해도 계속 실패하는 메시지(poison pill)는 `<topic>.DLT` 토픽으로
 * 보내 원본 토픽 파티션의 나머지 메시지가 막히지 않게 한다. 이 CommonErrorHandler
 * 빈은 Spring Boot가 자동 설정하는 KafkaListenerContainerFactory에 자동으로
 * 적용된다(별도 factory 커스터마이징 불필요).
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
