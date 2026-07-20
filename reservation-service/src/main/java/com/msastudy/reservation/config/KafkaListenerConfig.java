package com.msastudy.reservation.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

/**
 * Kafka 리스너 컨테이너 공통 동작 설정. Spring Boot는 여기 정의된 CommonErrorHandler /
 * RecordInterceptor 빈을 자동 설정된 KafkaListenerContainerFactory에 자동으로 적용한다
 * (별도 factory 커스터마이징 불필요).
 */
@Configuration
public class KafkaListenerConfig {

    private static final String MDC_KEY = "reservationId";

    /**
     * 메시지 처리가 실패하면 지수 백오프로 재시도하고, 재시도를 다 소진해도 계속
     * 실패하는 메시지(poison pill)는 `<topic>.DLT` 토픽으로 보내 원본 토픽 파티션의
     * 나머지 메시지가 막히지 않게 한다.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        return new DefaultErrorHandler(recoverer, backOff);
    }

    /**
     * Kafka 메시지 key는 항상 reservationId(Saga 상관관계 ID)이므로, 리스너가 메시지를
     * 처리하는 동안 이 값을 MDC에 심어 로그에 자동으로 찍히게 한다. Zipkin 같은 별도
     * 트레이싱 인프라 없이도, 하나의 Saga가 여러 서비스 로그에 걸쳐 어떻게 흘렀는지
     * reservationId로 grep해서 추적할 수 있다.
     */
    @Bean
    public RecordInterceptor<Object, Object> mdcRecordInterceptor() {
        return new RecordInterceptor<>() {
            @Override
            public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
                Object key = record.key();
                MDC.put(MDC_KEY, key != null ? key.toString() : "unknown");
                return record;
            }

            @Override
            public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
                MDC.remove(MDC_KEY);
            }

            @Override
            public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
                MDC.remove(MDC_KEY);
            }
        };
    }
}
