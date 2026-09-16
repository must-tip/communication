"""Django settings example matching the cloud-services Kafka workload contract.

The SDK accepts the server's current aliases, including KAFKA_SASL_USERNAME and
KAFKA_SSL_CA_LOCATION. Keep Secret *paths* in Django settings; the SDK reads the
mounted username/password files when it builds CommunicationSettings.
"""

from __future__ import annotations

import os


def env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


SERVICE_NAME = env("SERVICE_NAME", "service")

COMMUNICATION = {
    "SERVICE_NAME": SERVICE_NAME,
    "ENABLED_TRANSPORTS": env("COMMUNICATION_ENABLED_TRANSPORTS", "kafka"),
    "CONSUMER_TRANSPORTS": env("COMMUNICATION_CONSUMER_TRANSPORTS", "kafka"),
    "PRIMARY_TRANSPORT": env("COMMUNICATION_PRIMARY_TRANSPORT", "kafka"),
    "PUBLISH_MODE": env("COMMUNICATION_PUBLISH_MODE", "primary"),
    # Use a service-owned topic that already exists in the Kafka platform.
    "DEFAULT_TOPIC": env("COMMUNICATION_DEFAULT_TOPIC", "events"),
    "TOPIC_PREFIX": env("COMMUNICATION_TOPIC_PREFIX", ""),
    "REQUIRE_DURABLE_CONSUMERS": True,
}

# These names intentionally mirror scripts/service_manager.py in cloud-services.
KAFKA_BOOTSTRAP_SERVERS = env(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
)
KAFKA_SECURITY_PROTOCOL = env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISM = env("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_USERNAME = env("KAFKA_USERNAME")
KAFKA_SASL_USERNAME = env("KAFKA_SASL_USERNAME")
KAFKA_USERNAME_FILE = env("KAFKA_USERNAME_FILE", "/var/run/secrets/kafka-client/username")
KAFKA_PASSWORD_FILE = env("KAFKA_PASSWORD_FILE", "/var/run/secrets/kafka-client/password")
KAFKA_SSL_CA_LOCATION = env("KAFKA_SSL_CA_LOCATION", "/var/run/secrets/kafka/ca.crt")

# Optional client tuning. SDK safety invariants remain authoritative.
KAFKA = {
    "client.id": SERVICE_NAME,
    "compression.type": "zstd",
    "linger.ms": 5,
}
KAFKA_CONSUMER = {
    "partition.assignment.strategy": "cooperative-sticky",
}

# Database-backed inbox idempotency is recommended for production consumers.
COMMUNICATION_IDEMPOTENCY_BACKEND = "database"
