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
}

application {
    mainClass.set("com.msastudy.kafkalab.OrderingLabRunner")
}
