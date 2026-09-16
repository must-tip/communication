from __future__ import annotations

import asyncio
from typing import Iterable, Mapping

from .config import CommunicationSettings
from .errors import ConfigurationError, TransportUnavailable
from .protocols import EventTransport
from .transports import KafkaTransport, NatsTransport


class TransportRegistry:
    def __init__(self, settings: CommunicationSettings) -> None:
        self.settings = settings
        self._transports: dict[str, EventTransport] = {}
        self._lock: asyncio.Lock | None = None

    def register(self, transport: EventTransport, *, replace: bool = False) -> EventTransport:
        name = str(transport.name).strip().casefold()
        if not name:
            raise ConfigurationError("transport name is invalid")
        if name in self._transports and not replace:
            raise ConfigurationError(f"transport {name!r} is already registered")
        self._transports[name] = transport
        return transport

    def _build(self, name: str) -> EventTransport:
        if name == "kafka":
            return KafkaTransport(self.settings.kafka)
        if name == "nats":
            return NatsTransport(self.settings.nats)
        raise TransportUnavailable(f"transport {name!r} is not configured")

    def get(self, name: str) -> EventTransport:
        normalized = name.strip().casefold()
        transport = self._transports.get(normalized)
        if transport is None:
            transport = self.register(self._build(normalized))
        return transport

    def resolve(self, names: Iterable[str]) -> tuple[EventTransport, ...]:
        return tuple(self.get(name) for name in names)

    async def start(self, names: Iterable[str] | None = None) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            selected = self.resolve(names or self.settings.enabled_transports)
            started: list[EventTransport] = []
            try:
                for transport in selected:
                    await transport.start()
                    started.append(transport)
            except Exception:
                for transport in reversed(started):
                    await transport.close()
                raise

    async def close(self) -> None:
        await asyncio.gather(*(transport.close() for transport in self._transports.values()), return_exceptions=True)

    async def health(self) -> Mapping[str, object]:
        results = await asyncio.gather(
            *(transport.health() for transport in self._transports.values()),
            return_exceptions=True,
        )
        output: dict[str, object] = {}
        for name, result in zip(self._transports, results):
            output[name] = {"healthy": False, "error": type(result).__name__} if isinstance(result, BaseException) else result
        return output
