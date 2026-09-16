from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable, Mapping, Protocol, Sequence

from .envelope import EventEnvelope


@dataclass(frozen=True, slots=True)
class TransportPublishResult:
    transport: str
    topic: str
    event_id: str
    accepted: bool
    partition: int | None = None
    offset: int | None = None
    sequence: int | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PublishResult:
    event: EventEnvelope
    mode: str
    results: tuple[TransportPublishResult, ...]
    failures: Mapping[str, str] = field(default_factory=dict)

    @property
    def succeeded_transports(self) -> tuple[str, ...]:
        return tuple(item.transport for item in self.results if item.accepted)

    @property
    def successful(self) -> bool:
        if self.mode == "all":
            return bool(self.results) and not self.failures
        return bool(self.results)


@dataclass(slots=True)
class IncomingMessage:
    transport: str
    topic: str
    payload: bytes
    headers: Mapping[str, str]
    key: str | None = None
    partition: int | None = None
    offset: int | None = None
    sequence: int | None = None
    timestamp: datetime | None = None
    delivery_count: int = 1
    raw_message: object | None = field(default=None, repr=False)
    _ack: Callable[[], Awaitable[None]] | None = field(default=None, repr=False)
    _nack: Callable[[float | None], Awaitable[None]] | None = field(default=None, repr=False)
    _term: Callable[[], Awaitable[None]] | None = field(default=None, repr=False)

    async def ack(self) -> None:
        if self._ack is not None:
            await self._ack()

    async def nack(self, delay_seconds: float | None = None) -> None:
        if self._nack is not None:
            await self._nack(delay_seconds)

    async def term(self) -> None:
        if self._term is not None:
            await self._term()
        else:
            await self.ack()


MessageCallback = Callable[[IncomingMessage], Awaitable[None]]


class Subscription(Protocol):
    async def close(self) -> None:
        ...


class EventTransport(Protocol):
    name: str
    durable: bool

    async def start(self) -> None:
        ...

    async def close(self) -> None:
        ...

    async def flush(self, timeout: float = 10.0) -> None:
        ...

    async def publish(
        self,
        *,
        topic: str,
        payload: bytes,
        headers: Mapping[str, str],
        key: str | None,
        timeout: float,
    ) -> TransportPublishResult:
        ...

    async def subscribe(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        callback: MessageCallback,
        concurrency: int,
        max_pending: int,
    ) -> Subscription:
        ...

    async def health(self) -> Mapping[str, object]:
        ...
