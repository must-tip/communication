from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Sequence

from ..config import NatsSettings
from ..errors import ConfigurationError, TransportUnavailable
from ..protocols import IncomingMessage, MessageCallback, TransportPublishResult

logger = logging.getLogger(__name__)


def _ssl_context(config: NatsSettings) -> ssl.SSLContext | None:
    tls_requested = config.require_tls or any(server.startswith("tls://") for server in config.servers)
    if not tls_requested and not config.tls_ca_file:
        return None
    if config.tls_ca_file:
        ca = Path(config.tls_ca_file)
        if not ca.is_file():
            raise ConfigurationError("NATS TLS CA file does not exist")
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(ca))
    else:
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    if config.tls_cert_file and config.tls_key_file:
        cert = Path(config.tls_cert_file)
        key = Path(config.tls_key_file)
        if not cert.is_file() or not key.is_file():
            raise ConfigurationError("NATS mTLS certificate or key file does not exist")
        context.load_cert_chain(str(cert), str(key))
    return context


def _normalized_servers(config: NatsSettings) -> list[str]:
    result: list[str] = []
    for server in config.servers:
        if server.startswith("tls://"):
            result.append("nats://" + server[len("tls://") :])
        else:
            result.append(server)
    return result


@dataclass(slots=True)
class _NatsSubscription:
    subscriptions: list[Any]
    closed: bool = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for subscription in self.subscriptions:
            try:
                drain = getattr(subscription, "drain", None)
                if callable(drain):
                    await drain()
                else:
                    await subscription.unsubscribe()
            except Exception:
                logger.debug("NATS subscription close failed", exc_info=True)


class NatsTransport:
    name = "nats"

    def __init__(
        self,
        config: NatsSettings,
        *,
        connect_factory: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        self.config = config
        self.durable = bool(config.jetstream)
        self._connect_factory = connect_factory
        self._nc: Any = None
        self._js: Any = None
        self._lock: asyncio.Lock | None = None

    async def start(self) -> None:
        if self._nc is not None and getattr(self._nc, "is_connected", False):
            return
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._nc is not None and getattr(self._nc, "is_connected", False):
                return
            connect = self._connect_factory
            if connect is None:
                try:
                    import nats
                except ImportError as exc:
                    raise TransportUnavailable("nats-py is required for the NATS transport") from exc
                connect = nats.connect
            options: dict[str, Any] = {
                "servers": _normalized_servers(self.config),
                "name": self.config.name,
                "allow_reconnect": True,
                "max_reconnect_attempts": -1,
                "reconnect_time_wait": 1,
                "connect_timeout": 5,
                "ping_interval": 20,
                "max_outstanding_pings": 3,
                "no_echo": True,
            }
            options.update(dict(self.config.extra))
            tls = _ssl_context(self.config)
            if tls is not None:
                options["tls"] = tls
            if self.config.tls_hostname:
                options["tls_hostname"] = self.config.tls_hostname
            if self.config.token:
                options["token"] = self.config.token
            elif self.config.user_credentials:
                credentials = Path(self.config.user_credentials)
                if not credentials.is_file():
                    raise ConfigurationError("NATS credentials file does not exist")
                options["user_credentials"] = str(credentials)
            elif self.config.user:
                options["user"] = self.config.user
                options["password"] = self.config.password
            try:
                self._nc = await connect(**options)
                self._js = self._nc.jetstream() if self.config.jetstream else None
            except Exception as exc:
                self._nc = None
                self._js = None
                raise TransportUnavailable("NATS connection failed") from exc

    async def close(self) -> None:
        nc, self._nc, self._js = self._nc, None, None
        if nc is None:
            return
        try:
            if getattr(nc, "is_connected", False):
                await nc.drain()
            else:
                await nc.close()
        except Exception:
            logger.warning("NATS shutdown failed", exc_info=True)

    async def flush(self, timeout: float = 10.0) -> None:
        if self._nc is None:
            return
        await self._nc.flush(timeout=timeout)

    async def publish(
        self,
        *,
        topic: str,
        payload: bytes,
        headers: Mapping[str, str],
        key: str | None,
        timeout: float,
    ) -> TransportPublishResult:
        await self.start()
        transport_headers = dict(headers)
        if key:
            transport_headers.setdefault("message-key", key)
        event_id = str(transport_headers.get("event-id", ""))
        if self.config.jetstream:
            if self._js is None:
                raise TransportUnavailable("NATS JetStream is unavailable")
            transport_headers.setdefault("Nats-Msg-Id", event_id)
            try:
                ack = await self._js.publish(topic, payload, headers=transport_headers, timeout=timeout)
            except Exception as exc:
                raise TransportUnavailable("NATS JetStream publish failed") from exc
            return TransportPublishResult(
                transport=self.name,
                topic=topic,
                event_id=event_id,
                accepted=True,
                sequence=int(getattr(ack, "seq", 0) or 0) or None,
                metadata={"stream": getattr(ack, "stream", None), "duplicate": bool(getattr(ack, "duplicate", False))},
            )
        try:
            await self._nc.publish(topic, payload, headers=transport_headers)
            await self._nc.flush(timeout=timeout)
        except Exception as exc:
            raise TransportUnavailable("NATS core publish failed") from exc
        return TransportPublishResult(
            transport=self.name,
            topic=topic,
            event_id=event_id,
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
    ) -> _NatsSubscription:
        await self.start()
        subscriptions: list[Any] = []

        async def handle(msg: Any) -> None:
            metadata = None
            try:
                metadata = getattr(msg, "metadata", None)
            except Exception:
                metadata = None
            delivery_count = int(getattr(metadata, "num_delivered", 1) or 1)
            sequence = None
            try:
                sequence = int(getattr(getattr(metadata, "sequence", None), "stream", 0) or 0) or None
            except (TypeError, ValueError):
                sequence = None

            async def ack() -> None:
                method = getattr(msg, "ack", None)
                if callable(method):
                    await method()

            async def nack(delay_seconds: float | None = None) -> None:
                method = getattr(msg, "nak", None)
                if callable(method):
                    if delay_seconds is None:
                        await method()
                    else:
                        try:
                            await method(delay=delay_seconds)
                        except TypeError:
                            await method()

            async def term() -> None:
                method = getattr(msg, "term", None)
                if callable(method):
                    await method()
                else:
                    await ack()

            raw_headers = dict(getattr(msg, "headers", {}) or {})
            normalized_headers = {
                str(name).strip().casefold(): str(value).strip()
                for name, value in raw_headers.items()
                if value is not None
            }
            incoming = IncomingMessage(
                transport=self.name,
                topic=str(getattr(msg, "subject", "")),
                payload=bytes(getattr(msg, "data", b"")),
                headers=normalized_headers,
                key=normalized_headers.get("message-key"),
                sequence=sequence,
                timestamp=datetime.now(timezone.utc),
                delivery_count=delivery_count,
                raw_message=msg,
                _ack=ack,
                _nack=nack,
                _term=term,
            )
            await callback(incoming)

        if self.config.jetstream:
            if self._js is None:
                raise TransportUnavailable("NATS JetStream is unavailable")
            for topic in topics:
                durable = f"{group_id}-{topic}".replace(".", "-").replace("_", "-")[:128]
                for _worker in range(max(1, int(concurrency))):
                    kwargs: dict[str, Any] = {
                        "subject": topic,
                        "durable": durable,
                        "queue": group_id,
                        "cb": handle,
                        "manual_ack": True,
                        "pending_msgs_limit": max_pending,
                        "pending_bytes_limit": max_pending * 1_048_576,
                    }
                    # nats-py versions vary in whether ack_wait/max_deliver are accepted
                    # directly by subscribe; use them when supported and retain a safe fallback.
                    try:
                        subscription = await self._js.subscribe(
                            **kwargs,
                            ack_wait=self.config.ack_wait_seconds,
                            max_deliver=self.config.max_deliver,
                        )
                    except TypeError:
                        subscription = await self._js.subscribe(**kwargs)
                    subscriptions.append(subscription)
        else:
            for topic in topics:
                for _worker in range(max(1, int(concurrency))):
                    subscriptions.append(await self._nc.subscribe(topic, queue=group_id, cb=handle))
        return _NatsSubscription(subscriptions)

    async def health(self) -> Mapping[str, object]:
        connected = bool(self._nc is not None and getattr(self._nc, "is_connected", False))
        return {
            "transport": self.name,
            "healthy": connected,
            "durable": self.durable,
            "jetstream": self.config.jetstream,
        }
