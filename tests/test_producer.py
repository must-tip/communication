from __future__ import annotations

import asyncio
from typing import Mapping, Sequence

import pytest

from communication.bus import EventBus
from communication.config import CommunicationSettings
from communication.producer import AccountsProducer, publish_event
from communication.protocols import MessageCallback, TransportPublishResult
from communication.registry import TransportRegistry


class FakeSubscription:
    async def close(self):
        return None


class FakeTransport:
    name = "nats"
    durable = True

    def __init__(self):
        self.items = []

    async def start(self):
        return None

    async def close(self):
        return None

    async def flush(self, timeout=10):
        return None

    async def publish(self, *, topic, payload, headers, key, timeout):
        self.items.append((topic, payload, dict(headers), key))
        return TransportPublishResult("nats", topic, headers["event-id"], True)

    async def subscribe(self, *, topics: Sequence[str], group_id: str, callback: MessageCallback, concurrency: int, max_pending: int):
        return FakeSubscription()

    async def health(self) -> Mapping[str, object]:
        return {"healthy": True}


def build_producer():
    settings = CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "authentication",
            "ENABLED_TRANSPORTS": ("nats",),
            "PRIMARY_TRANSPORT": "nats",
            "CONSUMER_TRANSPORTS": ("nats",),
            "NATS": {"SERVERS": "nats://nats:4222"},
        }
    )
    registry = TransportRegistry(settings)
    transport = FakeTransport()
    registry.register(transport)
    return AccountsProducer(settings_obj=settings, event_bus=EventBus(settings, registry=registry)), transport


def test_sync_publish_and_callback_use_owned_runtime() -> None:
    producer, transport = build_producer()
    callback = []
    producer.publish_event(
        {"id": 1},
        topic="accounts.user.created",
        event_type="accounts.user.created",
        callback=lambda error, metadata: callback.append((error, metadata)),
    )
    producer.flush()
    producer.close()
    assert len(transport.items) == 1
    assert callback[0][0] is None
    assert callback[0][1]["transports"] == ("nats",)


def test_legacy_producer_signatures_and_module_function_preserve_topic_name() -> None:
    producer, transport = build_producer()
    previous = AccountsProducer._singleton
    AccountsProducer._singleton = producer
    try:
        assert producer.publish_event(
            "accounts.user.created",
            {"id": 1},
        )
        assert producer.publish_event(
            {"id": 2},
            "accounts.user.created",
        )
        assert AccountsProducer.publish_event(
            topic="accounts.user.created",
            event={"id": 3},
        )
        assert publish_event(
            data={"id": 4},
            topic="accounts.user.created",
        )
    finally:
        AccountsProducer._singleton = previous
        producer.close()

    assert [item[0] for item in transport.items] == [
        "accounts.user.created",
        "accounts.user.created",
        "accounts.user.created",
        "accounts.user.created",
    ]


def test_legacy_constructor_and_produce_method_remain_supported() -> None:
    settings = CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "authentication",
            "ENABLED_TRANSPORTS": ("nats",),
            "PRIMARY_TRANSPORT": "nats",
            "CONSUMER_TRANSPORTS": ("nats",),
            "NATS": {"SERVERS": "nats://nats:4222"},
        }
    )
    registry = TransportRegistry(settings)
    transport = FakeTransport()
    registry.register(transport)
    producer = AccountsProducer(
        settings_obj=settings,
        event_bus=EventBus(settings, registry=registry),
        producer_conf={
            "bootstrap.servers": "kafka-bootstrap.kafka-system.svc.cluster.local:9092",
            "client.id": "rbac",
            "linger.ms": 7,
        },
        serializer=lambda value, context=None: b"validated",
        default_topic="rbac.events",
    )
    try:
        assert (
            producer.settings.kafka.bootstrap_servers
            == "kafka-bootstrap.kafka-system.svc.cluster.local:9092"
        )
        assert producer.settings.kafka.client_id == "rbac"
        assert producer.settings.kafka.producer["linger.ms"] == 7
        assert producer.produce("rbac.events", {"id": 1}, trace_id="trace-1")
    finally:
        producer.close()
    assert transport.items[0][0] == "rbac.events"


@pytest.mark.asyncio
async def test_async_publish_uses_same_safe_runtime() -> None:
    producer, transport = build_producer()
    await asyncio.gather(
        *(producer.publish_event_async({"id": index}, topic="accounts.user.created", event_type="accounts.user.created") for index in range(10))
    )
    producer.close()
    assert len(transport.items) == 10
