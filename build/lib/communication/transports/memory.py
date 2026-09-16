from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping, Sequence

from ..protocols import IncomingMessage, MessageCallback, TransportPublishResult


@dataclass(slots=True)
class _MemorySubscription:
    transport: "MemoryTransport"
    registrations: list[tuple[str, MessageCallback]]
    closed: bool = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for registration in self.registrations:
            try:
                self.transport._subscriptions.remove(registration)
            except ValueError:
                pass


class MemoryTransport:
    """Deterministic in-process transport for tests and local development."""

    name = "memory"
    durable = False

    def __init__(self) -> None:
        self.started = False
        self.published: list[tuple[str, bytes, Mapping[str, str], str | None]] = []
        self._subscriptions: list[tuple[str, MessageCallback]] = []

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.started = False
        self._subscriptions.clear()

    async def flush(self, timeout: float = 10.0) -> None:
        return None

    async def publish(
        self,
        *,
        topic: str,
        payload: bytes,
        headers: Mapping[str, str],
        key: str | None,
        timeout: float,
    ) -> TransportPublishResult:
        if not self.started:
            raise RuntimeError("memory transport is not started")
        self.published.append((topic, payload, dict(headers), key))
        callbacks = [callback for registered_topic, callback in self._subscriptions if registered_topic == topic]
        for callback in callbacks:
            message = IncomingMessage(
                transport=self.name,
                topic=topic,
                payload=payload,
                headers=dict(headers),
                key=key,
                timestamp=datetime.now(timezone.utc),
            )
            await callback(message)
        return TransportPublishResult(
            transport=self.name,
            topic=topic,
            event_id=str(headers.get("event-id", "")),
            accepted=True,
        )

    async def subscribe(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        callback: MessageCallback,
        concurrency: int,
        max_pending: int,
    ) -> _MemorySubscription:
        registrations = [(topic, callback) for topic in topics]
        self._subscriptions.extend(registrations)
        return _MemorySubscription(self, registrations)

    async def health(self) -> Mapping[str, object]:
        return {"transport": self.name, "healthy": self.started, "durable": self.durable}
