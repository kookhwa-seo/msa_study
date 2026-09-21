plugins {
    id("org.springframework.boot")
    java
}

// Spring Boot 3.4.x와 짝을 맞춘 Spring Cloud 2024.0.x(Moorgate) BOM. Eureka 클라이언트로
// 등록하기 위해서만 필요하다 - 서비스 간 통신(Kafka)에는 영향 없음.
dependencyManagement {
    imports {
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2024.0.0")
    }
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.cloud:spring-cloud-starter-netflix-eureka-client")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    implementation("com.github.loki4j:loki-logback-appender:1.5.2")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.springframework.kafka:spring-kafka-test")
}
