plugins {
    id("org.springframework.boot")
    java
}

// 블로킹(RestClient) vs 논블로킹(WebClient) 비교 실습(docs/blocking-vs-nonblocking-lab.md) 전용.
// starter-web + starter-webflux를 같이 두면 Spring Boot는 DispatcherServlet이 클래스패스에
// 있는 걸 보고 서블릿(Tomcat) 스택으로 뜬다 - 그래도 WebClient(webflux가 들고 오는 reactor-netty
// 클라이언트)는 정상적으로 쓸 수 있다. 즉 "같은 Tomcat 플랫폼 스레드 위에서" 블로킹 호출과
// 논블로킹 호출을 나란히 비교한다는 이번 실습의 전제를 그대로 만족한다.
dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-webflux")
}
