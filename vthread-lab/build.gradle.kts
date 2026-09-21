plugins {
    java
    application
}

// 외부 의존성 없이 java.util.concurrent만 쓴다 - Spring/Tomcat 같은 프레임워크 레이어를 끼우면
// 커넥션 풀/스레드 풀 설정이 또 하나의 변수가 돼서, "가상 스레드 vs 플랫폼 스레드" 자체의
// 차이를 보기 어려워진다. JDK 21의 java.lang.Thread.ofVirtual()/Executors만으로 순수 비교한다.

application {
    mainClass.set("com.msastudy.vthreadlab.VirtualThreadLabRunner")
}
