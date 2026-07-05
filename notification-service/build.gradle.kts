plugins {
    id("org.springframework.boot")
    java
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter")
    // ObjectMapper 자동 설정(JacksonAutoConfiguration)은 spring-web의 Jackson2ObjectMapperBuilder를
    // 필요로 한다. 웹 서버(embedded tomcat)는 필요 없으므로 starter-web 대신 starter-json만 추가.
    implementation("org.springframework.boot:spring-boot-starter-json")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    testImplementation("org.springframework.kafka:spring-kafka-test")
}
