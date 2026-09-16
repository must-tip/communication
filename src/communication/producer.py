from __future__ import annotations

import functools
import logging
import threading
from dataclasses import replace
from typing import Any, Callable, Dict, Mapping, Optional, Union

from .bus import EventBus
from .config import CommunicationSettings, _normalized_kafka_mapping
from .lifecycle import AsyncRuntime
from .protocols import PublishResult

logger = logging.getLogger("communication.producer")
Data = Union[Dict[str, Any], list[Any], str, int, float, bool, bytes, None]
DeliveryCallback = Callable[[Optional[Exception], Optional[Dict[str, Any]]], None]
_MISSING = object()


class _shared_instance_method:
    """Bind to an object normally and to a process singleton on the class."""

    def __init__(self, function: Callable[..., Any]) -> None:
        self.function = function
        functools.update_wrapper(self, function)

    def __get__(self, instance: object, owner: type["AccountsProducer"]):
        if instance is not None:
            return self.function.__get__(instance, owner)

        @functools.wraps(self.function)
        def shared(*args: object, **kwargs: object):
            return self.function(owner._shared_instance(), *args, **kwargs)

        return shared


class AccountsProducer:
    """Backward-compatible producer backed by the transport-neutral EventBus.

    Existing application code uses both constructed producers and
    ``AccountsProducer.publish_event(...)`` as a process-wide convenience API.
    The shared-instance descriptor preserves both forms while all publication
    still goes through the same validated event envelope and transport policy.
    """

    _singleton: "AccountsProducer | None" = None
    _singleton_lock = threading.RLock()

    def __init__(
        self,
        *,
        default_schema: Optional[Dict[str, Any]] = None,
        settings_obj: CommunicationSettings | None = None,
        event_bus: EventBus | None = None,
        producer_conf: Optional[Mapping[str, Any]] = None,
        serializer: Optional[Callable[..., bytes]] = None,
        default_topic: Optional[str] = None,
    ) -> None:
        self._default_schema = default_schema
        self._serializer = serializer
        resolved_settings = settings_obj or CommunicationSettings.from_django_settings()
        if producer_conf:
            normalized = _normalized_kafka_mapping(
                producer_conf,
                name="producer_conf",
                extras_target="PRODUCER",
            )
            kafka = replace(
                resolved_settings.kafka,
                bootstrap_servers=normalized.get(
                    "BOOTSTRAP_SERVERS",
                    resolved_settings.kafka.bootstrap_servers,
                ),
                client_id=str(
                    normalized.get("CLIENT_ID", resolved_settings.kafka.client_id)
                ),
                producer={
                    **dict(resolved_settings.kafka.producer),
                    **dict(normalized.get("PRODUCER") or {}),
                },
                security_protocol=str(
                    normalized.get(
                        "SECURITY_PROTOCOL",
                        resolved_settings.kafka.security_protocol,
                    )
                ),
                sasl_mechanism=normalized.get(
                    "SASL_MECHANISM",
                    resolved_settings.kafka.sasl_mechanism,
                ),
                username=normalized.get(
                    "USERNAME",
                    resolved_settings.kafka.username,
                ),
                password=normalized.get(
                    "PASSWORD",
                    resolved_settings.kafka.password,
                ),
                ssl_ca_location=normalized.get(
                    "SSL_CA_LOCATION",
                    resolved_settings.kafka.ssl_ca_location,
                ),
                transactional_id=normalized.get(
                    "TRANSACTIONAL_ID",
                    resolved_settings.kafka.transactional_id,
                ),
                retries=int(
                    normalized.get("RETRIES", resolved_settings.kafka.retries)
                ),
                retry_backoff_seconds=float(
                    normalized.get(
                        "RETRY_BACKOFF_SECONDS",
                        resolved_settings.kafka.retry_backoff_seconds,
                    )
                ),
                acks=str(normalized.get("ACKS", resolved_settings.kafka.acks)),
                extra_producer_config={
                    **dict(resolved_settings.kafka.extra_producer_config),
                    **dict(normalized.get("EXTRA_PRODUCER_CONFIG") or {}),
                },
            )
            resolved_settings = replace(resolved_settings, kafka=kafka)
        if default_topic is not None:
            resolved_settings = replace(
                resolved_settings,
                default_topic=resolved_settings.canonical_topic(default_topic),
            )
        self.settings = resolved_settings
        self._event_bus = event_bus
        self._runtime = AsyncRuntime(name="accounts-event-producer")
        self._closed = False

    @classmethod
    def _shared_instance(cls) -> "AccountsProducer":
        with cls._singleton_lock:
            if cls._singleton is None or cls._singleton._closed:
                cls._singleton = cls()
            return cls._singleton

    @staticmethod
    def _publish_arguments(
        data: object,
        positional: tuple[object, ...],
        *,
        topic: Optional[str],
        event: object,
        value: object,
    ) -> tuple[Data, Optional[str]]:
        aliases = [item for item in (event, value) if item is not _MISSING]
        if len(aliases) > 1 or (aliases and data is not _MISSING):
            raise TypeError("publish_event received more than one payload value")
        payload = aliases[0] if aliases else data
        if payload is _MISSING:
            raise TypeError("publish_event requires event data")
        if len(positional) > 1:
            raise TypeError("publish_event accepts at most two positional arguments")
        if positional:
            second = positional[0]
            if isinstance(payload, str) and not isinstance(second, str):
                if topic is not None:
                    raise TypeError("publish_event received topic more than once")
                topic = payload
                payload = second
            elif isinstance(second, str):
                if topic is not None:
                    raise TypeError("publish_event received topic more than once")
                topic = second
            else:
                raise TypeError("the second positional publish_event argument must be a topic")
        return payload, topic  # type: ignore[return-value]

    async def _bus(self) -> EventBus:
        if self._event_bus is None:
            self._event_bus = EventBus(self.settings)
        return self._event_bus

    async def _publish(
        self,
        data: Data,
        *,
        topic: Optional[str],
        friendly_name: Optional[str],
        key: Optional[str],
        headers: Optional[Mapping[str, object]],
        trace_id: Optional[str],
        event_type: Optional[str],
        correlation_id: Optional[str],
        causation_id: Optional[str],
        tenant_id: Optional[str],
        event_id: Optional[str],
        mode: Optional[str],
        transports: Optional[tuple[str, ...]],
        timeout: Optional[float],
    ) -> PublishResult:
        bus = await self._bus()
        selected_topic = topic or friendly_name or self.settings.default_topic
        selected_type = event_type or friendly_name or selected_topic
        event = bus.build_event(
            event_type=selected_type,
            data=data,
            topic=selected_topic,
            event_id=event_id,
            trace_id=trace_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            tenant_id=tenant_id,
            headers=headers,
        )
        return await bus.publish(
            event,
            mode=mode,
            transports=transports,
            key=key,
            timeout=timeout,
        )

    @_shared_instance_method
    def publish_event(
        self,
        data: Data | object = _MISSING,
        *positional: object,
        topic: Optional[str] = None,
        friendly_name: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        callback: Optional[DeliveryCallback] = None,
        event_type: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        event_id: Optional[str] = None,
        mode: Optional[str] = None,
        transports: Optional[tuple[str, ...]] = None,
        event: object = _MISSING,
        value: object = _MISSING,
        attempts: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        del attempts  # Retries are governed by the validated transport settings.
        if self._closed:
            raise RuntimeError("producer is closed")
        data, topic = self._publish_arguments(
            data,
            positional,
            topic=topic,
            event=event,
            value=value,
        )
        try:
            result = self._runtime.run(
                self._publish(
                    data,
                    topic=topic,
                    friendly_name=friendly_name,
                    key=key,
                    headers=headers,
                    trace_id=trace_id,
                    event_type=event_type,
                    correlation_id=correlation_id,
                    causation_id=causation_id,
                    tenant_id=tenant_id,
                    event_id=event_id,
                    mode=mode,
                    transports=transports,
                    timeout=timeout,
                ),
                timeout=(timeout or self.settings.publish_timeout_seconds) + 5.0,
            )
            if callback:
                callback(
                    None,
                    {
                        "topic": result.event.topic,
                        "partition": result.results[0].partition if result.results else None,
                        "offset": result.results[0].offset if result.results else None,
                        "key": key,
                        "event_id": result.event.event_id,
                        "transports": result.succeeded_transports,
                    },
                )
            return True
        except Exception as exc:
            logger.exception("event publication failed")
            if callback:
                try:
                    callback(exc, None)
                except Exception:
                    logger.exception("delivery callback raised")
            raise

    @_shared_instance_method
    async def publish_event_async(
        self,
        data: Data | object = _MISSING,
        *positional: object,
        topic: Optional[str] = None,
        friendly_name: Optional[str] = None,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        event_type: Optional[str] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        event_id: Optional[str] = None,
        mode: Optional[str] = None,
        transports: Optional[tuple[str, ...]] = None,
        event: object = _MISSING,
        value: object = _MISSING,
        attempts: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> PublishResult:
        del attempts
        if self._closed:
            raise RuntimeError("producer is closed")
        data, topic = self._publish_arguments(
            data,
            positional,
            topic=topic,
            event=event,
            value=value,
        )
        return await self._runtime.run_async(
            self._publish(
                data,
                topic=topic,
                friendly_name=friendly_name,
                key=key,
                headers=headers,
                trace_id=trace_id,
                event_type=event_type,
                correlation_id=correlation_id,
                causation_id=causation_id,
                tenant_id=tenant_id,
                event_id=event_id,
                mode=mode,
                transports=transports,
                timeout=timeout,
            ),
            timeout=(timeout or self.settings.publish_timeout_seconds) + 5.0,
        )

    def produce(
        self,
        topic: str,
        data: Data,
        *,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        trace_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bool:
        """Preserve the configured producer object's topic-first API."""

        return self.publish_event(
            data,
            topic,
            key=key,
            headers=headers,
            trace_id=trace_id,
            timeout=timeout,
        )

    def flush(self, timeout: float = 10.0) -> None:
        if self._closed:
            return

        async def flush_bus() -> None:
            if self._event_bus is not None:
                await self._event_bus.flush(timeout)

        self._runtime.run(flush_bus(), timeout=timeout + 1.0)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        async def close_bus() -> None:
            if self._event_bus is not None:
                await self._event_bus.close()

        try:
            self._runtime.run(close_bus(), timeout=self.settings.graceful_shutdown_seconds)
        finally:
            self._runtime.stop(timeout=self.settings.graceful_shutdown_seconds)

    def __enter__(self) -> "AccountsProducer":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


def publish_event(
    data: Data | object = _MISSING,
    *positional: object,
    topic: Optional[str] = None,
    **kwargs: Any,
) -> bool:
    """Preserve the configured module-level publication function."""

    return AccountsProducer.publish_event(data, *positional, topic=topic, **kwargs)
