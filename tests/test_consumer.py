from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from communication.config import CommunicationSettings
from communication.consumer import ConsumerService
from communication.envelope import EventEnvelope
from communication.idempotency import MemoryIdempotencyStore
from communication.protocols import IncomingMessage


class FakeBus:
    def __init__(self) -> None:
        self.dlq = []

    async def publish_raw(self, **kwargs):
        self.dlq.append(kwargs)
        return SimpleNamespace()

    async def close(self):
        return None


def settings() -> CommunicationSettings:
    return CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "authentication",
            "ENABLED_TRANSPORTS": ("nats",),
            "PRIMARY_TRANSPORT": "nats",
            "CONSUMER_TRANSPORTS": ("nats",),
            "HANDLER_RETRIES": 1,
            "RETRY_BACKOFF_SECONDS": 0,
            "NATS": {"SERVERS": "nats://nats:4222"},
        }
    )


def message_for(event: EventEnvelope, state: dict[str, int]) -> IncomingMessage:
    async def ack():
        state["ack"] += 1

    async def nack(_delay=None):
        state["nack"] += 1

    async def term():
        state["term"] += 1

    return IncomingMessage(
        transport="nats",
        topic=event.topic,
        payload=event.to_bytes(),
        headers=event.transport_headers(),
        _ack=ack,
        _nack=nack,
        _term=term,
    )


@pytest.mark.asyncio
async def test_successful_handler_acks_and_duplicate_is_acked() -> None:
    calls = []

    def handler(payload, meta):
        calls.append(payload)
        return True

    bus = FakeBus()
    service = ConsumerService(
        ["accounts.user.created"],
        "accounts",
        handler=handler,
        settings_obj=settings(),
        event_bus=bus,
        idempotency_store=MemoryIdempotencyStore(),
    )
    event = EventEnvelope(event_type="accounts.user.created", data={"id": 1}, source="authentication", topic="accounts.user.created")
    state = {"ack": 0, "nack": 0, "term": 0}
    await service._process_message(message_for(event, state))
    await service._process_message(message_for(event, state))
    assert calls == [{"id": 1}]
    assert state["ack"] == 2
    assert state["nack"] == 0


@pytest.mark.asyncio
async def test_poison_event_is_sent_to_dlq_and_terminated() -> None:
    def handler(payload, meta):
        raise RuntimeError("do not leak this text")

    bus = FakeBus()
    service = ConsumerService(
        ["accounts.user.created"],
        "accounts",
        handler=handler,
        settings_obj=settings(),
        event_bus=bus,
        idempotency_store=MemoryIdempotencyStore(),
    )
    event = EventEnvelope(event_type="accounts.user.created", data={"id": 1}, source="authentication", topic="accounts.user.created")
    state = {"ack": 0, "nack": 0, "term": 0}
    await service._process_message(message_for(event, state))
    assert len(bus.dlq) == 1
    assert bus.dlq[0]["topic"] == "accounts.user.created.dlq"
    assert bus.dlq[0]["headers"]["x-dlq-reason"] == "RuntimeError"
    assert state["term"] == 1


def test_ack_failure_retries_commit_without_rehandling_or_dlq() -> None:
    async def scenario() -> None:
        calls = []
        acknowledgements = 0

        def handler(payload, meta):
            calls.append(payload)
            return True

        async def ack():
            nonlocal acknowledgements
            acknowledgements += 1
            if acknowledgements == 1:
                raise RuntimeError("simulated broker commit failure")

        async def nack(_delay=None):
            return None

        bus = FakeBus()
        service = ConsumerService(
            ["accounts.user.created"],
            "accounts",
            handler=handler,
            settings_obj=settings(),
            event_bus=bus,
            idempotency_store=MemoryIdempotencyStore(),
        )
        event = EventEnvelope(
            event_type="accounts.user.created",
            data={"id": 1},
            source="authentication",
            topic="accounts.user.created",
        )

        first = IncomingMessage(
            transport="kafka",
            topic=event.topic,
            payload=event.to_bytes(),
            headers=event.transport_headers(),
            partition=0,
            offset=10,
            _ack=ack,
            _nack=nack,
        )
        with pytest.raises(RuntimeError, match="simulated broker commit failure"):
            await service._process_message(first)

        second = IncomingMessage(
            transport="kafka",
            topic=event.topic,
            payload=event.to_bytes(),
            headers=event.transport_headers(),
            partition=0,
            offset=10,
            _ack=ack,
            _nack=nack,
        )
        await service._process_message(second)

        assert calls == [{"id": 1}]
        assert acknowledgements == 2
        assert bus.dlq == []

    asyncio.run(scenario())


def test_identical_legacy_payloads_at_distinct_offsets_are_not_false_duplicates() -> None:
    async def scenario() -> None:
        calls = []
        acknowledgements = 0

        def handler(payload, meta):
            calls.append((payload, meta["offset"]))
            return True

        async def ack():
            nonlocal acknowledgements
            acknowledgements += 1

        service = ConsumerService(
            ["accounts.user.created"],
            "accounts",
            handler=handler,
            settings_obj=settings(),
            event_bus=FakeBus(),
            idempotency_store=MemoryIdempotencyStore(),
        )
        payload = b'{"id":1}'
        for offset in (10, 11, 10):
            await service._process_message(
                IncomingMessage(
                    transport="kafka",
                    topic="accounts.user.created",
                    payload=payload,
                    headers={"content-type": "application/json"},
                    partition=0,
                    offset=offset,
                    _ack=ack,
                )
            )

        assert calls == [({"id": 1}, 10), ({"id": 1}, 11)]
        assert acknowledgements == 3

    asyncio.run(scenario())
