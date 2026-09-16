from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from .config import CommunicationSettings
from .envelope import EventEnvelope, normalize_headers, sign_payload
from .errors import PublishError
from .protocols import (
    MessageCallback,
    PublishResult,
    Subscription,
    TransportPublishResult,
)
from .registry import TransportRegistry

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CompositeSubscription:
    subscriptions: list[Subscription]
    closed: bool = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        await asyncio.gather(*(item.close() for item in self.subscriptions), return_exceptions=True)


class EventBus:
    def __init__(
        self,
        settings: CommunicationSettings,
        *,
        registry: TransportRegistry | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry or TransportRegistry(settings)
        self._started: set[str] = set()
        self._start_lock: asyncio.Lock | None = None

    async def _ensure_started(self, names: Iterable[str]) -> None:
        selected = tuple(dict.fromkeys(name.casefold() for name in names))
        missing = [name for name in selected if name not in self._started]
        if not missing:
            return
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        async with self._start_lock:
            missing = [name for name in selected if name not in self._started]
            if missing:
                await self.registry.start(missing)
                self._started.update(missing)

    def build_event(
        self,
        *,
        event_type: str,
        data: object,
        topic: str | None = None,
        event_id: str | None = None,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        tenant_id: str | None = None,
        headers: Mapping[str, object] | None = None,
    ) -> EventEnvelope:
        kwargs: dict[str, object] = {
            "event_type": event_type,
            "data": data,
            "source": self.settings.service_name,
            "topic": self.settings.canonical_topic(topic),
            "schema_version": self.settings.schema_version,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "tenant_id": tenant_id,
            "headers": normalize_headers(
                headers,
                maximum_headers=self.settings.max_headers,
                maximum_value_length=self.settings.max_header_value_length,
            ),
        }
        if event_id is not None:
            kwargs["event_id"] = event_id
        return EventEnvelope(**kwargs)

    def _ordered_names(self, names: Sequence[str] | None = None) -> tuple[str, ...]:
        selected = tuple(
            dict.fromkeys(
                str(name).strip().casefold()
                for name in (names or self.settings.enabled_transports)
                if str(name).strip()
            )
        )
        if self.settings.primary_transport in selected:
            return (self.settings.primary_transport,) + tuple(
                name for name in selected if name != self.settings.primary_transport
            )
        return selected

    async def publish(
        self,
        event: EventEnvelope,
        *,
        mode: str | None = None,
        transports: Sequence[str] | None = None,
        key: str | None = None,
        timeout: float | None = None,
    ) -> PublishResult:
        payload = event.to_bytes(maximum_bytes=self.settings.max_event_bytes)
        headers = event.transport_headers()
        if self.settings.signing_key_id:
            secret = self.settings.signing_keys[self.settings.signing_key_id]
            headers.update(sign_payload(payload, secret=secret, key_id=self.settings.signing_key_id))
        return await self.publish_raw(
            topic=event.topic,
            payload=payload,
            headers=headers,
            event_id=event.event_id,
            event=event,
            mode=mode,
            transports=transports,
            key=key or event.event_id,
            timeout=timeout,
        )

    async def emit(self, *, event_type: str, data: object, **kwargs: object) -> PublishResult:
        publish_keys = {"mode", "transports", "key", "timeout"}
        event_kwargs = {key: value for key, value in kwargs.items() if key not in publish_keys}
        publish_kwargs = {key: value for key, value in kwargs.items() if key in publish_keys}
        event = self.build_event(event_type=event_type, data=data, **event_kwargs)
        return await self.publish(event, **publish_kwargs)

    async def publish_raw(
        self,
        *,
        topic: str,
        payload: bytes,
        headers: Mapping[str, str],
        event_id: str,
        event: EventEnvelope | None = None,
        mode: str | None = None,
        transports: Sequence[str] | None = None,
        key: str | None = None,
        timeout: float | None = None,
    ) -> PublishResult:
        selected_mode = str(mode or self.settings.publish_mode).casefold()
        if selected_mode not in {"all", "any", "failover", "primary"}:
            raise PublishError(f"unsupported publish mode: {selected_mode}")
        names = self._ordered_names(transports)
        if not names:
            raise PublishError("at least one publishing transport is required")
        canonical_topic = self.settings.canonical_topic(topic)
        if not isinstance(payload, bytes):
            raise PublishError("event payload must be bytes")
        if not payload or len(payload) > self.settings.max_event_bytes:
            raise PublishError("event payload size is invalid")
        try:
            timeout_value = float(
                self.settings.publish_timeout_seconds if timeout is None else timeout
            )
        except (TypeError, ValueError) as exc:
            raise PublishError("publish timeout must be numeric") from exc
        if not 0.01 <= timeout_value <= 300.0:
            raise PublishError("publish timeout must be between 0.01 and 300 seconds")
        await self._ensure_started(names)
        failures: dict[str, str] = {}
        results: list[TransportPublishResult] = []

        async def send(name: str) -> TransportPublishResult:
            transport = self.registry.get(name)
            return await transport.publish(
                topic=canonical_topic,
                payload=payload,
                headers=headers,
                key=key,
                timeout=timeout_value,
            )

        if selected_mode in {"all", "any"}:
            outcomes = await asyncio.gather(*(send(name) for name in names), return_exceptions=True)
            for name, outcome in zip(names, outcomes):
                if isinstance(outcome, BaseException):
                    failures[name] = type(outcome).__name__
                else:
                    results.append(outcome)
            if selected_mode == "all" and failures:
                raise PublishError(f"event {event_id} was not delivered to every configured transport")
            if selected_mode == "any" and not results:
                raise PublishError(f"event {event_id} could not be delivered")
        elif selected_mode == "failover":
            for name in names:
                try:
                    results.append(await send(name))
                    break
                except Exception as exc:
                    failures[name] = type(exc).__name__
            if not results:
                raise PublishError(f"event {event_id} could not be delivered by failover transports")
        elif selected_mode == "primary":
            name = self.settings.primary_transport if self.settings.primary_transport in names else names[0]
            try:
                results.append(await send(name))
            except Exception as exc:
                failures[name] = type(exc).__name__
                raise PublishError(f"event {event_id} could not be delivered by the primary transport") from exc
        if event is None:
            event = EventEnvelope(
                event_id=event_id,
                event_type=str(headers.get("event-type") or canonical_topic),
                schema_version=str(headers.get("event-version") or self.settings.schema_version),
                source=str(headers.get("event-source") or self.settings.service_name),
                topic=canonical_topic,
                data={"raw": True},
            )
        return PublishResult(event=event, mode=selected_mode, results=tuple(results), failures=failures)

    async def subscribe(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        callback: MessageCallback,
        transports: Sequence[str] | None = None,
        concurrency: int | None = None,
        max_pending: int | None = None,
    ) -> CompositeSubscription:
        canonical_topics = tuple(self.settings.canonical_topic(topic) for topic in topics)
        names = tuple(
            dict.fromkeys(
                str(name).strip().casefold()
                for name in (transports or self.settings.consumer_transports)
                if str(name).strip()
            )
        )
        if not names:
            raise RuntimeError("at least one consumer transport is required")
        await self._ensure_started(names)
        subscriptions: list[Subscription] = []
        try:
            for name in names:
                transport = self.registry.get(name)
                if self.settings.require_durable_consumers and not transport.durable:
                    raise RuntimeError(f"transport {name} does not provide durable consumption")
                subscriptions.append(
                    await transport.subscribe(
                        topics=canonical_topics,
                        group_id=group_id,
                        callback=callback,
                        concurrency=concurrency or self.settings.consumer_concurrency,
                        max_pending=max_pending or self.settings.consumer_max_pending,
                    )
                )
        except Exception:
            await asyncio.gather(*(item.close() for item in subscriptions), return_exceptions=True)
            raise
        return CompositeSubscription(subscriptions)

    async def flush(self, timeout: float | None = None) -> None:
        transports = self.registry.resolve(self._started)
        await asyncio.gather(
            *(transport.flush(timeout or self.settings.flush_timeout_seconds) for transport in transports)
        )

    async def health(self) -> Mapping[str, object]:
        """Return redacted transport health for readiness and diagnostics."""

        return await self.registry.health()

    async def close(self) -> None:
        await self.registry.close()
        self._started.clear()
