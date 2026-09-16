from __future__ import annotations

import pytest

from communication.envelope import EventEnvelope
from communication.handlers import EventContext, HandlerRegistry


@pytest.mark.asyncio
async def test_registry_dispatches_priority_order_and_wildcards() -> None:
    registry = HandlerRegistry()
    calls = []

    @registry.register("accounts.*", priority=1)
    def broad(event, context):
        calls.append("broad")

    @registry.register("accounts.user.created", priority=10)
    async def exact(event, context):
        calls.append("exact")

    event = EventEnvelope(event_type="accounts.user.created", data={}, source="authentication", topic="accounts.user.created")
    await registry.dispatch(event, EventContext(transport="memory", topic=event.topic, headers={}))
    assert calls == ["exact", "broad"]
