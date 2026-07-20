plugins {
    id("org.springframework.boot")
    java
}

// Spring Cloud Gateway는 WebFlux 기반이라 Boot BOM만으로는 버전이 안 맞는다.
// Spring Boot 3.4.x와 짝을 맞춘 Spring Cloud 2024.0.x(Moorgate) BOM을 추가로 가져온다.
dependencyManagement {
    imports {
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2024.0.0")
    }
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
}
