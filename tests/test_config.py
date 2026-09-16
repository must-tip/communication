from __future__ import annotations

from types import SimpleNamespace

import pytest

from communication.config import CommunicationSettings, KafkaSettings, NatsSettings
from communication.errors import ConfigurationError


def dual_settings(**overrides):
    raw = {
        "SERVICE_NAME": "authentication",
        "ENABLED_TRANSPORTS": ("kafka", "nats"),
        "PRIMARY_TRANSPORT": "kafka",
        "PUBLISH_MODE": "all",
        "CONSUMER_TRANSPORTS": ("kafka", "nats"),
        "KAFKA": {"BOOTSTRAP_SERVERS": "kafka:9092"},
        "NATS": {"SERVERS": "nats://nats:4222", "JETSTREAM": True},
    }
    raw.update(overrides)
    return CommunicationSettings.from_mapping(raw)


def test_dual_transport_configuration_is_valid() -> None:
    config = dual_settings()
    assert config.enabled_transports == ("kafka", "nats")
    assert config.publish_mode == "all"
    assert config.canonical_topic("accounts.user.created") == "accounts.user.created"


def test_durable_consumption_rejects_core_nats() -> None:
    with pytest.raises(ConfigurationError):
        dual_settings(REQUIRE_DURABLE_CONSUMERS=True, NATS={"SERVERS": "nats://nats:4222", "JETSTREAM": False})


def test_kafka_legacy_topic_api_remains_available() -> None:
    config = KafkaSettings(bootstrap_servers="localhost:9092")
    assert config.full_topic_name("user.created") == "svc.v1.user.created"
    assert config.dlq_topic_name("user.created") == "svc.v1.user.created.dlq"
    assert config.producer_config()["enable.idempotence"] is True


def test_platform_django_kafka_settings_are_loaded_without_plaintext_fallback() -> None:
    platform = {
        "bootstrap.servers": "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "authentication",
        "sasl.password": "secret",
        "ssl.ca.location": "/var/run/secrets/kafka/ca.crt",
        "client.id": "authentication",
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "zstd",
        "request.timeout.ms": 30_000,
    }
    django_settings = SimpleNamespace(
        COMMUNICATION={
            "ENABLED_TRANSPORTS": ("kafka",),
            "PRIMARY_TRANSPORT": "kafka",
            "CONSUMER_TRANSPORTS": ("kafka",),
        },
        KAFKA=platform,
        KAFKA_CONSUMER={
            **platform,
            "group.id": "configured-group-is-not-authoritative",
            "enable.auto.commit": True,
        },
    )

    config = CommunicationSettings.from_django_settings(django_settings)

    assert config.kafka.bootstrap_servers == platform["bootstrap.servers"]
    assert config.kafka.security_protocol == "SASL_SSL"
    assert config.kafka.sasl_mechanism == "SCRAM-SHA-512"
    assert config.kafka.ssl_ca_location == "/var/run/secrets/kafka/ca.crt"
    consumer = config.kafka.consumer_config("accounts-service")
    assert consumer["group.id"] == "accounts-service"
    assert consumer["enable.auto.commit"] is False
    assert "enable.idempotence" not in consumer
    assert "delivery.timeout.ms" not in consumer
    assert "compression.type" not in consumer


def test_kafka_client_safety_invariants_cannot_be_disabled_by_extra_options() -> None:
    config = KafkaSettings(
        bootstrap_servers="kafka-bootstrap.kafka-system.svc.cluster.local:9092",
        client_id="authentication",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        username="authentication",
        password="secret",
        ssl_ca_location="/var/run/secrets/kafka/ca.crt",
        producer={
            "acks": "0",
            "enable.idempotence": False,
            "allow.auto.create.topics": True,
            "security.protocol": "PLAINTEXT",
        },
        consumer={
            "group.id": "wrong-group",
            "enable.auto.commit": True,
            "enable.auto.offset.store": True,
            "allow.auto.create.topics": True,
            "isolation.level": "read_uncommitted",
            "security.protocol": "PLAINTEXT",
        },
    )

    producer = config.producer_config()
    assert producer["acks"] == "all"
    assert producer["enable.idempotence"] is True
    assert producer["allow.auto.create.topics"] is False
    assert producer["security.protocol"] == "SASL_SSL"

    consumer = config.consumer_config("accounts-service")
    assert consumer["group.id"] == "accounts-service"
    assert consumer["enable.auto.commit"] is False
    assert consumer["enable.auto.offset.store"] is False
    assert consumer["allow.auto.create.topics"] is False
    assert consumer["isolation.level"] == "read_committed"
    assert consumer["security.protocol"] == "SASL_SSL"


def test_kafka_sasl_ssl_requires_platform_ca_and_credential_free_host_port() -> None:
    with pytest.raises(ConfigurationError):
        KafkaSettings(
            bootstrap_servers="kafka:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            username="authentication",
            password="secret",
        )
    with pytest.raises(ConfigurationError):
        KafkaSettings(bootstrap_servers="SASL_SSL://user:secret@kafka:9092")


def test_nats_require_tls_rejects_plaintext_or_mixed_servers() -> None:
    with pytest.raises(ConfigurationError):
        NatsSettings(servers=("nats://nats:4222",), require_tls=True)
    with pytest.raises(ConfigurationError):
        NatsSettings(
            servers=("tls://nats-a:4222", "nats://nats-b:4222"),
            require_tls=False,
        )

    settings = NatsSettings(servers=("tls://nats:4222",), require_tls=True)
    assert settings.require_tls is True


def test_django_csv_transports_select_first_transport_as_primary() -> None:
    config = CommunicationSettings.from_django_settings({
        "COMMUNICATION": {"ENABLED_TRANSPORTS": "kafka,nats"},
    })
    assert config.enabled_transports == ("kafka", "nats")
    assert config.primary_transport == "kafka"
