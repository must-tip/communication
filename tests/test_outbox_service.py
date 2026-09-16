from __future__ import annotations

from types import SimpleNamespace

from communication.config import CommunicationSettings
from communication.envelope import EventEnvelope
from communication.outbox import OutboxService


def test_enqueue_on_commit_persists_immediately_in_callers_transaction() -> None:
    settings = CommunicationSettings.from_mapping(
        {
            "ENABLED_TRANSPORTS": ("nats",),
            "PRIMARY_TRANSPORT": "nats",
            "CONSUMER_TRANSPORTS": ("nats",),
            "NATS": {"SERVERS": "nats://nats:4222"},
        }
    )
    service = OutboxService(settings, event_bus=SimpleNamespace())
    event = EventEnvelope(
        event_type="accounts.user.created",
        source="authentication",
        topic="accounts.user.created",
        data={"id": 1},
    )
    calls = []
    service.enqueue = lambda *args, **kwargs: calls.append((args, kwargs))

    service.enqueue_on_commit(event, transports=("nats",), using="events")

    assert calls == [((event,), {"transports": ("nats",), "using": "events"})]
