plugins {
    id("org.springframework.boot")
    java
}

// Spring Boot 3.4.x와 짝을 맞춘 Spring Cloud 2024.0.x(Moorgate) BOM. api-gateway와 동일한
// 버전을 쓴다 - Eureka 서버/클라이언트/게이트웨이가 서로 다른 Spring Cloud 버전이면 호환성
// 문제가 생길 수 있어서 프로젝트 전체가 하나의 버전을 맞춰 쓴다.
dependencyManagement {
    imports {
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2024.0.0")
    }
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-netflix-eureka-server")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
}
