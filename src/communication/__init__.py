from __future__ import annotations

from .bus import EventBus
from .config import CommunicationSettings, KafkaSettings, NatsSettings
from .consumer import ConsumerService, create_consumer_service, start_consumer_in_background
from .diagnostics import KafkaDiagnosticReport, run_kafka_roundtrip
from .envelope import EventEnvelope
from .handlers import EventContext, HandlerRegistry, event_handler
from .producer import AccountsProducer, publish_event
from .serializer import (
    deserialize_event,
    deserialize_payload,
    serialize_event,
    serialize_payload,
)

__all__ = [
    "AccountsProducer",
    "CommunicationSettings",
    "ConsumerService",
    "EventBus",
    "EventContext",
    "EventEnvelope",
    "HandlerRegistry",
    "KafkaDiagnosticReport",
    "KafkaSettings",
    "NatsSettings",
    "create_consumer_service",
    "deserialize_event",
    "deserialize_payload",
    "event_handler",
    "publish_event",
    "run_kafka_roundtrip",
    "serialize_event",
    "serialize_payload",
    "start_consumer_in_background",
]

default_app_config = "communication.apps.CommunicationConfig"
