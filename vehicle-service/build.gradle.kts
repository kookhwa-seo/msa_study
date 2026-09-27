plugins {
    id("org.springframework.boot")
    java
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.github.loki4j:loki-logback-appender:1.5.2")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.kafka:spring-kafka-test")
}

// 동시 배정 방어 실습(VehicleAssignmentConcurrencyTest)은 실제 로컬 vehicle-db(Postgres)가
// 떠 있어야 한다 - 인프라 없이도 항상 통과해야 하는 기본 ./gradlew test에서는 빼고,
// 별도 태스크로만 돌린다. kafka-lab/deadlock-lab/vthread-lab이 별도 run 태스크인 것과 같은 이유.
tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("concurrencyTest") {
    description = "동시 배정 방어(비관적/낙관적 락 vs 락 없음) 실습. docker compose up -d vehicle-db 필요."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    testLogging {
        showStandardStreams = true
        events("passed", "failed", "skipped")
    }
    outputs.upToDateWhen { false }
}
