plugins {
    java
}

// common is a plain library (shared event contracts), not a runnable Spring Boot app
tasks.findByName("bootJar")?.let { it.enabled = false }
tasks.named<Jar>("jar") {
    enabled = true
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
}
