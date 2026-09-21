rootProject.name = "msa-rental-study"

include(
    "common",
    "reservation-service",
    "vehicle-service",
    "payment-service",
    "notification-service",
    "api-gateway",
    "llm-service",
    "eureka-server",
    "kafka-lab",
    "deadlock-lab",
    "vthread-lab"
)
