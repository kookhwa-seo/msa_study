# 오토스케일링 실습(로컬 k8s HPA)을 위한 이미지. 빌드는 호스트에서 ./gradlew :reservation-service:bootJar로
# 미리 만든 jar를 그대로 복사하기만 한다 - 이미지 안에서 Gradle을 또 돌리지 않아 빌드가 훨씬 빠르다.
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app
COPY build/libs/reservation-service-0.1.0.jar app.jar

EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
