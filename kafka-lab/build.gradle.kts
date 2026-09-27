plugins {
    java
    application
}

// Spring 없이 순수 kafka-clients로만 작성한다 (학습 목적: Spring Kafka가 감춰주는
// 파티션 배정/컨슈머 그룹 join/커밋을 직접 다루면서 "왜 partition key가 순서를
// 좌우하는지"를 코드 레벨에서 확인하기 위함). 버전은 spring-boot-dependencies BOM을
// 그대로 따른다 (루트 build.gradle.kts의 subprojects 블록에서 이미 적용됨).
dependencies {
    implementation("org.apache.kafka:kafka-clients")
    implementation("org.slf4j:slf4j-simple")
    // PaymentAuthorizedFloodProducer가 실제 common 모듈의 PaymentAuthorizedEvent/Topics를
    // 그대로 써서, vehicle-service가 실제로 받는 것과 100% 같은 모양의 메시지를 만든다.
    implementation(project(":common"))
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
}

application {
    mainClass.set("com.msastudy.kafkalab.OrderingLabRunner")
}

// 파티션 수 x 컨슈머 수 처리량 실습(docs/consumer-throughput-lab.md) 전용 진입점.
// OrderingLabRunner와는 별개 실습이라 application 플러그인의 기본 mainClass 대신
// 별도 태스크로 뺐다 - vehicle-service의 concurrencyTest와 같은 이유.
tasks.register<JavaExec>("floodPaymentAuthorized") {
    group = "application"
    description = "payment-events 토픽에 PaymentAuthorized 이벤트를 대량 발행 (컨슈머 처리량 실습용)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.msastudy.kafkalab.PaymentAuthorizedFloodProducer")
}
