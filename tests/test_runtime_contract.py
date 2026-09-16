from __future__ import annotations

from pathlib import Path

import pytest

from communication.config import CommunicationSettings, KafkaSettings
from communication.errors import ConfigurationError


def _write(path: Path, value: str) -> str:
    path.write_text(value, encoding="utf-8")
    return str(path)


def test_cloud_services_environment_contract_loads_secret_files_and_aliases(tmp_path: Path) -> None:
    username = _write(tmp_path / "username", "repo-notification\n")
    password = _write(tmp_path / "password", "super-secret-value\n")
    ca = _write(tmp_path / "ca.crt", "-----BEGIN CERTIFICATE-----\nplaceholder\n-----END CERTIFICATE-----\n")
    env = {
        "SERVICE_NAME": "repo-notification",
        "COMMUNICATION_ENABLED_TRANSPORTS": "kafka",
        "COMMUNICATION_CONSUMER_TRANSPORTS": "kafka",
        "COMMUNICATION_PRIMARY_TRANSPORT": "kafka",
        "KAFKA_BOOTSTRAP_SERVERS": "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
        "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
        "KAFKA_SASL_USERNAME": "repo-notification",
        "KAFKA_USERNAME_FILE": username,
        "KAFKA_PASSWORD_FILE": password,
        "KAFKA_SSL_CA_LOCATION": ca,
    }

    settings = CommunicationSettings.from_environment(env)

    assert settings.service_name == "repo-notification"
    assert settings.enabled_transports == ("kafka",)
    assert settings.kafka.bootstrap_servers == "kafka-bootstrap.kafka-system.svc.cluster.local:9092"
    assert settings.kafka.username == "repo-notification"
    assert settings.kafka.password == "super-secret-value"
    assert settings.kafka.ssl_ca_location == ca
    assert settings.kafka.security_protocol == "SASL_SSL"
    assert settings.kafka.sasl_mechanism == "SCRAM-SHA-512"


def test_environment_contract_accepts_authentication_username_name(tmp_path: Path) -> None:
    password = _write(tmp_path / "password", "super-secret-value")
    ca = _write(tmp_path / "ca.crt", "certificate")
    settings = CommunicationSettings.from_environment(
        {
            "SERVICE_NAME": "repo-authentication",
            "COMMUNICATION_ENABLED_TRANSPORTS": "kafka",
            "KAFKA_BOOTSTRAP_SERVERS": "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "KAFKA_USERNAME": "repo-authentication",
            "KAFKA_PASSWORD_FILE": password,
            "KAFKA_CA_FILE": ca,
        }
    )
    assert settings.kafka.username == "repo-authentication"
    assert settings.kafka.ssl_ca_location == ca


def test_environment_contract_fails_closed_when_required_secret_is_missing(tmp_path: Path) -> None:
    ca = _write(tmp_path / "ca.crt", "certificate")
    with pytest.raises(ConfigurationError, match="KAFKA_PASSWORD"):
        CommunicationSettings.from_environment(
            {
                "SERVICE_NAME": "service",
                "COMMUNICATION_ENABLED_TRANSPORTS": "kafka",
                "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
                "KAFKA_USERNAME": "service",
                "KAFKA_CA_FILE": ca,
            }
        )


def test_tls_and_topic_safety_cannot_be_overridden() -> None:
    config = KafkaSettings(
        bootstrap_servers="kafka:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        username="service",
        password="secret",
        ssl_ca_location="/var/run/secrets/kafka/ca.crt",
        producer={
            "enable.ssl.certificate.verification": False,
            "ssl.endpoint.identification.algorithm": "none",
            "allow.auto.create.topics": True,
        },
        consumer={
            "enable.ssl.certificate.verification": False,
            "ssl.endpoint.identification.algorithm": "none",
            "allow.auto.create.topics": True,
        },
    )
    for mapping in (config.producer_config(), config.consumer_config("service-consumer")):
        assert mapping["enable.ssl.certificate.verification"] is True
        assert mapping["ssl.endpoint.identification.algorithm"] == "https"
        assert mapping["allow.auto.create.topics"] is False


def test_consumer_callback_timeout_must_leave_rebalance_margin() -> None:
    with pytest.raises(ConfigurationError, match="leave at least 30 seconds"):
        KafkaSettings(
            bootstrap_servers="kafka:9092",
            consumer_callback_timeout_seconds=880,
            consumer_max_poll_interval_ms=900_000,
        )
