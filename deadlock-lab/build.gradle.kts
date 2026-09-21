plugins {
    java
    application
}

// kafka-lab과 같은 원칙: Spring/JPA 없이 순수 JDBC로 작성한다 - 트랜잭션 락(SELECT ... FOR UPDATE)과
// Postgres의 데드락 탐지를 직접 다루면서 그 메커니즘 자체를 보기 위함. 버전은 spring-boot-dependencies
// BOM을 따른다(루트 build.gradle.kts의 subprojects 블록에서 이미 적용됨).
dependencies {
    implementation("org.postgresql:postgresql")
    implementation("org.slf4j:slf4j-simple")
}

application {
    mainClass.set("com.msastudy.deadlocklab.DeadlockLabRunner")
}
