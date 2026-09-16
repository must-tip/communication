"""Copy the relevant values into the project settings module.

Secrets shown as ``get_secret(...)`` are expected to use the project's existing
Vault-backed settings helper. Do not place broker credentials directly in source.
"""

import os


def get_secret(name: str) -> str | None:
    """Example secret resolver; production projects should read mounted files."""

    return os.getenv(name)


def get_config(name: str, default: str) -> str:
    return os.getenv(name, default)


SERVICE_NAME = os.getenv("SERVICE_NAME", "calls-service")

COMMUNICATION = {
    "SERVICE_NAME": SERVICE_NAME,
    # One or both: ("kafka",), ("nats",), or ("kafka", "nats").
    "ENABLED_TRANSPORTS": ("kafka", "nats"),
    "PRIMARY_TRANSPORT": "kafka",
    # primary: primary only
    # failover: primary then secondary
    # all: both must acknowledge
    # any: publish to both and accept one acknowledgement
    "PUBLISH_MODE": "all",
    # Consumers can run one or both. Shared idempotency is mandatory for both.
    "CONSUMER_TRANSPORTS": ("kafka", "nats"),
    "DEFAULT_TOPIC": "accounts.events",
    "TOPIC_PREFIX": "musttip",
    "SCHEMA_VERSION": "v1",
    "MAX_EVENT_BYTES": 1024 * 1024,
    "PUBLISH_TIMEOUT_SECONDS": 5,
    "FLUSH_TIMEOUT_SECONDS": 10,
    "HANDLER_RETRIES": 3,
    "RETRY_BACKOFF_SECONDS": 0.25,
    "RETRY_MAX_SECONDS": 30,
    "CONSUMER_CONCURRENCY": 4,
    "CONSUMER_MAX_PENDING": 2000,
    "GRACEFUL_SHUTDOWN_SECONDS": 30,
    "REQUIRE_DURABLE_CONSUMERS": True,
    "IDEMPOTENCY_TTL_SECONDS": 86400,
    "IDEMPOTENCY_FAIL_CLOSED": True,
    "SIGNING_REQUIRED": True,
    "SIGNING_KEY_ID": "events-2026-01",
    "SIGNING_KEYS": {
        "events-2026-01": get_secret("COMMUNICATION_EVENT_SIGNING_KEY"),
    },
    "KAFKA": {
        "BOOTSTRAP_SERVERS": get_config(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
        ),
        "CLIENT_ID": SERVICE_NAME,
        "SECURITY_PROTOCOL": "SASL_SSL",
        "SASL_MECHANISM": "SCRAM-SHA-512",
        "USERNAME": get_secret("KAFKA_USERNAME"),
        "PASSWORD": get_secret("KAFKA_PASSWORD"),
        "SSL_CA_LOCATION": get_config(
            "KAFKA_CA_FILE",
            "/var/run/secrets/kafka/ca.crt",
        ),
        "PRODUCER": {
            "compression.type": "zstd",
            "linger.ms": 5,
            "batch.num.messages": 10000,
        },
        "CONSUMER": {
            "partition.assignment.strategy": "cooperative-sticky",
        },
    },
    "NATS": {
        "SERVERS": get_config("NATS_SERVERS", "tls://nats:4222"),
        "NAME": SERVICE_NAME,
        "TOKEN": get_secret("NATS_TOKEN"),
        "REQUIRE_TLS": True,
        "TLS_CA_FILE": get_config("NATS_CA_FILE", "/run/certs/nats-ca.pem"),
        "JETSTREAM": True,
        "STREAM": "MUSTTIP_EVENTS",
        "ACK_WAIT_SECONDS": 60,
        "MAX_DELIVER": 5,
    },
}

# django-cache is suitable only when the configured cache is shared and durable
# enough for the expected redelivery window. "database" uses InboxEvent rows.
COMMUNICATION_IDEMPOTENCY_BACKEND = "database"
