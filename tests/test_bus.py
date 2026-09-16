from __future__ import annotations

import asyncio
from typing import Mapping, Sequence

import pytest

from communication.bus import EventBus
from communication.config import CommunicationSettings
from communication.errors import PublishError
from communication.protocols import MessageCallback, TransportPublishResult
from communication.registry import TransportRegistry


class FakeSubscription:
    async def close(self) -> None:
        return None


class FakeTransport:
    durable = True

    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.started = False
        self.events = []

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.started = False

    async def flush(self, timeout: float = 10.0) -> None:
        return None

    async def publish(self, *, topic, payload, headers, key, timeout):
        if self.fail:
            raise RuntimeError("unavailable")
        self.events.append((topic, payload, dict(headers), key))
        return TransportPublishResult(self.name, topic, headers["event-id"], True)

    async def subscribe(self, *, topics: Sequence[str], group_id: str, callback: MessageCallback, concurrency: int, max_pending: int):
        return FakeSubscription()

    async def health(self) -> Mapping[str, object]:
        return {"healthy": not self.fail}


def config(mode: str) -> CommunicationSettings:
    return CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "authentication",
            "ENABLED_TRANSPORTS": ("kafka", "nats"),
            "PRIMARY_TRANSPORT": "kafka",
            "PUBLISH_MODE": mode,
            "CONSUMER_TRANSPORTS": ("kafka", "nats"),
            "KAFKA": {"BOOTSTRAP_SERVERS": "kafka:9092"},
            "NATS": {"SERVERS": "nats://nats:4222"},
        }
    )


@pytest.mark.asyncio
async def test_all_mode_delivers_same_event_to_both_transports() -> None:
    settings = config("all")
    registry = TransportRegistry(settings)
    kafka, nats = FakeTransport("kafka"), FakeTransport("nats")
    registry.register(kafka)
    registry.register(nats)
    bus = EventBus(settings, registry=registry)
    event = bus.build_event(event_type="accounts.user.created", data={"user_id": "1"}, topic="accounts.user.created")
    result = await bus.publish(event)
    assert result.successful
    assert result.succeeded_transports == ("kafka", "nats")
    assert kafka.events[0][2]["event-id"] == nats.events[0][2]["event-id"]


@pytest.mark.asyncio
async def test_failover_uses_secondary_after_primary_failure() -> None:
    settings = config("failover")
    registry = TransportRegistry(settings)
    registry.register(FakeTransport("kafka", fail=True))
    nats = FakeTransport("nats")
    registry.register(nats)
    bus = EventBus(settings, registry=registry)
    result = await bus.emit(event_type="accounts.user.created", data={"user_id": "1"}, topic="accounts.user.created")
    assert result.successful
    assert result.succeeded_transports == ("nats",)
    assert result.failures == {"kafka": "RuntimeError"}


@pytest.mark.asyncio
async def test_all_mode_fails_when_one_transport_fails() -> None:
    settings = config("all")
    registry = TransportRegistry(settings)
    registry.register(FakeTransport("kafka"))
    registry.register(FakeTransport("nats", fail=True))
    bus = EventBus(settings, registry=registry)
    with pytest.raises(PublishError):
        await bus.emit(event_type="accounts.user.created", data={}, topic="accounts.user.created")


def test_health_report_contains_no_configuration_secrets() -> None:
    async def scenario() -> None:
        settings = config("all")
        registry = TransportRegistry(settings)
        registry.register(FakeTransport("kafka"))
        registry.register(FakeTransport("nats", fail=True))
        bus = EventBus(settings, registry=registry)
        await bus._ensure_started(("kafka", "nats"))

        health = await bus.health()

        assert health == {
            "kafka": {"healthy": True},
            "nats": {"healthy": False},
        }
        assert "password" not in repr(health).casefold()
        await bus.close()

    asyncio.run(scenario())
