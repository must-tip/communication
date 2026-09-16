from __future__ import annotations


def test_legacy_and_corrected_import_paths_resolve() -> None:
    from communication.config import KafkaSettings
    from communication.producer import AccountsProducer as CorrectProducer
    from communication.producer import AccountsProducer as LegacyProducer

    assert CorrectProducer is LegacyProducer
    assert KafkaSettings(bootstrap_servers="localhost:9092")


def test_optional_broker_dependencies_are_lazy() -> None:
    import communication
    from communication.transports.kafka import KafkaTransport
    from communication.transports.nats import NatsTransport

    assert communication.EventBus
    assert KafkaTransport.name == "kafka"
    assert NatsTransport.name == "nats"


def test_configured_compatibility_exports_resolve() -> None:
    import communication
    from communication.nat.notification_service import (
        publish_notification_event_async,
        publish_notification_event_sync,
    )
    from communication.producer import publish_event

    assert communication.default_app_config == "communication.apps.CommunicationConfig"
    assert communication.serialize_payload
    assert communication.deserialize_payload
    assert publish_event
    assert publish_notification_event_async
    assert publish_notification_event_sync
