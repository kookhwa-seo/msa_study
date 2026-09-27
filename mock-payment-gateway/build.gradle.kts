plugins {
    id("org.springframework.boot")
    java
}

// 블로킹 vs 논블로킹 비교 실습(docs/blocking-vs-nonblocking-lab.md) 전용 - 실제 결제망을
// 흉내 내는 가짜 "외부" 서버. 이 프로젝트의 Saga 참여자(reservation/vehicle/payment-service)와는
// 무관하고, 진짜 PG사 API처럼 "일부러 느리게 응답하는 제3자 HTTP 서버" 역할만 한다.
dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
}
