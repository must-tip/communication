from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from ..config import CommunicationSettings, NatsSettings
from ..transports.nats import NatsTransport

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PublishJob:
    subject: str
    payload: bytes
    headers: Mapping[str, str]
    timeout: float
    future: asyncio.Future[None]


def _settings_from_legacy_conf(conf: Optional[Dict[str, Any]]) -> NatsSettings:
    try:
        settings = CommunicationSettings.from_django_settings().nats
    except Exception:
        # Keep this compatibility utility importable in standalone scripts and
        # tests; real Django deployments still resolve the project settings.
        settings = NatsSettings()
    if not conf:
        return settings
    servers = conf.get("servers") or settings.servers
    if isinstance(servers, str):
        servers = tuple(item.strip() for item in servers.split(",") if item.strip())
    return NatsSettings(
        servers=tuple(servers),
        name=str(conf.get("name") or settings.name),
        token=conf.get("token") or settings.token,
        user_credentials=conf.get("user_credentials") or settings.user_credentials,
        user=conf.get("user") or settings.user,
        password=conf.get("password") or settings.password,
        tls_ca_file=settings.tls_ca_file,
        tls_cert_file=settings.tls_cert_file,
        tls_key_file=settings.tls_key_file,
        tls_hostname=conf.get("tls_hostname") or settings.tls_hostname,
        require_tls=settings.require_tls,
        jetstream=settings.jetstream,
        stream=settings.stream,
        publish_timeout_seconds=settings.publish_timeout_seconds,
        ack_wait_seconds=settings.ack_wait_seconds,
        max_deliver=settings.max_deliver,
        extra={key: value for key, value in conf.items() if key not in {"servers", "name", "token", "user_credentials", "user", "password", "tls_hostname"}},
    )


class PublisherHighThroughput:
    """Backward-compatible buffered NATS publisher with real drain semantics."""

    def __init__(
        self,
        *,
        conf: Optional[Dict[str, Any]] = None,
        worker_count: int = 4,
        queue_maxsize: int = 10_000,
        dlq_enabled: bool = True,
        dlq_suffix: str = ".failed",
        metrics_enabled: bool = False,
        flush_interval: float = 0.05,
        flush_every: int = 200,
    ) -> None:
        self._transport = NatsTransport(_settings_from_legacy_conf(conf))
        self._worker_count = max(1, min(int(worker_count), 128))
        self._queue_maxsize = max(1, int(queue_maxsize))
        self._dlq_enabled = bool(dlq_enabled)
        self._dlq_suffix = str(dlq_suffix or ".failed")
        self._metrics_enabled = bool(metrics_enabled)
        self._flush_interval = max(0.001, float(flush_interval))
        self._flush_every = max(1, int(flush_every))
        self._queue: asyncio.Queue[PublishJob | None] | None = None
        self._workers: list[asyncio.Task[None]] = []
        self._running = False
        self._lock: asyncio.Lock | None = None
        self._published = 0
        self._failed = 0
        self._dlq_published = 0

    @property
    def running(self) -> bool:
        return self._running

    @property
    def metrics(self) -> Mapping[str, int]:
        return {
            "published": self._published,
            "failed": self._failed,
            "dlq_published": self._dlq_published,
            "queued": self._queue.qsize() if self._queue else 0,
        }

    async def start(self) -> None:
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._running:
                return
            await self._transport.start()
            self._queue = asyncio.Queue(maxsize=self._queue_maxsize)
            self._running = True
            self._workers = [
                asyncio.create_task(self._worker(index), name=f"nats-buffered-publisher-{index}")
                for index in range(self._worker_count)
            ]

    async def publish_async(
        self,
        *,
        subject: str,
        payload: bytes,
        headers: Optional[Dict[str, str]] = None,
        use_jetstream: bool = False,
        timeout: float = 2.0,
    ) -> None:
        if not self._running or self._queue is None:
            raise RuntimeError("Publisher not started: call await get_publisher().start() at startup")
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError("payload must be bytes-like")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        normalized_headers = dict(headers or {})
        normalized_headers.setdefault("event-id", str(uuid.uuid4()))
        job = PublishJob(
            subject=str(subject).strip(),
            payload=bytes(payload),
            headers=normalized_headers,
            timeout=max(0.1, float(timeout)),
            future=future,
        )
        try:
            await asyncio.wait_for(self._queue.put(job), timeout=job.timeout)
            await asyncio.wait_for(future, timeout=job.timeout + 1.0)
        except asyncio.TimeoutError as exc:
            if not future.done():
                future.cancel()
            raise TimeoutError("publisher queue or delivery timed out") from exc

    async def _worker(self, index: int) -> None:
        assert self._queue is not None
        since_flush = 0
        while True:
            job = await self._queue.get()
            try:
                if job is None:
                    return
                try:
                    await self._transport.publish(
                        topic=job.subject,
                        payload=job.payload,
                        headers=job.headers,
                        key=job.headers.get("event-id"),
                        timeout=job.timeout,
                    )
                    self._published += 1
                    since_flush += 1
                    if since_flush >= self._flush_every:
                        await self._transport.flush(job.timeout)
                        since_flush = 0
                    if not job.future.done():
                        job.future.set_result(None)
                except Exception as exc:
                    self._failed += 1
                    if self._dlq_enabled and not job.subject.endswith(self._dlq_suffix):
                        try:
                            dlq_headers = dict(job.headers)
                            dlq_headers["x-dlq-reason"] = type(exc).__name__
                            await self._transport.publish(
                                topic=f"{job.subject}{self._dlq_suffix}",
                                payload=job.payload,
                                headers=dlq_headers,
                                key=job.headers.get("event-id"),
                                timeout=job.timeout,
                            )
                            self._dlq_published += 1
                        except Exception:
                            logger.exception("buffered publisher DLQ failed", extra={"worker": index})
                    if not job.future.done():
                        job.future.set_exception(exc)
            finally:
                self._queue.task_done()

    async def flush(self, timeout: float = 10.0) -> None:
        if self._queue is None:
            return
        await asyncio.wait_for(self._queue.join(), timeout=max(0.1, timeout))
        await self._transport.flush(timeout)

    async def stop(self, timeout: float = 30.0) -> None:
        if not self._running:
            return
        self._running = False
        try:
            await self.flush(timeout)
        except Exception:
            logger.warning("buffered publisher drain timed out", exc_info=True)
        if self._queue is not None:
            for _ in self._workers:
                await self._queue.put(None)
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._queue = None
        await self._transport.close()
