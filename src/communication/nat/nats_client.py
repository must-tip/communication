from __future__ import annotations

import asyncio
import signal
import threading
import weakref
from typing import Any, Dict, Optional

from ..config import CommunicationSettings, NatsSettings
from ..transports.nats import NatsTransport

_wrappers: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, NatsWrapper]" = weakref.WeakKeyDictionary()
_wrappers_lock = threading.RLock()


def get_default_conf() -> Dict[str, Any]:
    settings = CommunicationSettings.from_django_settings().nats
    result: Dict[str, Any] = {
        "servers": list(settings.servers),
        "name": settings.name,
    }
    if settings.token:
        result["token"] = settings.token
    elif settings.user_credentials:
        result["user_credentials"] = settings.user_credentials
    elif settings.user:
        result.update(user=settings.user, password=settings.password)
    return result


def _from_conf(conf: Optional[Dict[str, Any]]) -> NatsSettings:
    base = CommunicationSettings.from_django_settings().nats
    if not conf:
        return base
    servers = conf.get("servers") or base.servers
    if isinstance(servers, str):
        servers = tuple(item.strip() for item in servers.split(",") if item.strip())
    return NatsSettings(
        servers=tuple(servers),
        name=str(conf.get("name") or base.name),
        token=conf.get("token") or base.token,
        user_credentials=conf.get("user_credentials") or base.user_credentials,
        user=conf.get("user") or base.user,
        password=conf.get("password") or base.password,
        tls_ca_file=base.tls_ca_file,
        tls_cert_file=base.tls_cert_file,
        tls_key_file=base.tls_key_file,
        tls_hostname=conf.get("tls_hostname") or base.tls_hostname,
        require_tls=base.require_tls,
        jetstream=base.jetstream,
        stream=base.stream,
        publish_timeout_seconds=base.publish_timeout_seconds,
        ack_wait_seconds=base.ack_wait_seconds,
        max_deliver=base.max_deliver,
        extra={key: value for key, value in conf.items() if key not in {"servers", "name", "token", "user_credentials", "user", "password", "tls_hostname"}},
    )


class NatsWrapper:
    """Per-event-loop NATS connection wrapper retained for compatibility."""

    def __init__(self, conf: Optional[Dict[str, Any]] = None) -> None:
        self._conf = dict(conf or {})
        self.transport = NatsTransport(_from_conf(conf))

    async def connect(self):
        await self.transport.start()
        return self.transport._nc

    async def publish(self, subject: str, payload: bytes, headers: Optional[Dict[str, str]] = None) -> None:
        await self.transport.start()
        await self.transport._nc.publish(subject, payload, headers=headers)
        await self.transport._nc.flush(timeout=self.transport.config.publish_timeout_seconds)

    async def request(self, subject: str, payload: bytes, timeout: float = 1.0) -> bytes:
        await self.transport.start()
        message = await self.transport._nc.request(subject, payload, timeout=timeout)
        return bytes(getattr(message, "data", b""))

    async def drain(self) -> None:
        await self.transport.flush(10.0)

    async def close(self) -> None:
        await self.transport.close()

    def _run_ephemeral(self, operation: str, subject: str, payload: bytes, timeout: float = 1.0):
        result: dict[str, object] = {}
        done = threading.Event()

        def target() -> None:
            async def run():
                wrapper = NatsWrapper(self._conf)
                try:
                    if operation == "publish":
                        await wrapper.publish(subject, payload)
                        return None
                    return await wrapper.request(subject, payload, timeout)
                finally:
                    await wrapper.close()

            try:
                result["value"] = asyncio.run(run())
            except BaseException as exc:
                result["error"] = exc
            finally:
                done.set()

        thread = threading.Thread(target=target, name="nats-sync-operation", daemon=True)
        thread.start()
        if not done.wait(timeout=max(5.0, timeout + 5.0)):
            raise TimeoutError("Sync NATS operation timed out")
        if "error" in result:
            raise result["error"]  # type: ignore[misc]
        return result.get("value")

    def publish_sync(self, subject: str, payload: bytes, conf: Optional[Dict[str, Any]] = None) -> None:
        if conf:
            return NatsWrapper(conf)._run_ephemeral("publish", subject, payload)
        return self._run_ephemeral("publish", subject, payload)

    def request_sync(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 1.0,
        conf: Optional[Dict[str, Any]] = None,
    ) -> bytes:
        if conf:
            return NatsWrapper(conf)._run_ephemeral("request", subject, payload, timeout)  # type: ignore[return-value]
        return self._run_ephemeral("request", subject, payload, timeout)  # type: ignore[return-value]

    def _try_update_conf_if_unconnected(self, conf: Dict[str, Any]) -> None:
        if conf and self.transport._nc is None:
            self._conf = dict(conf)
            self.transport = NatsTransport(_from_conf(conf))


def _wrapper(conf: Optional[Dict[str, Any]] = None) -> NatsWrapper:
    loop = asyncio.get_running_loop()
    with _wrappers_lock:
        wrapper = _wrappers.get(loop)
        if wrapper is None:
            wrapper = NatsWrapper(conf)
            _wrappers[loop] = wrapper
        return wrapper


async def get_singleton(conf: Optional[Dict[str, Any]] = None):
    return await _wrapper(conf).connect()


async def nats_publish(subject: str, payload: bytes, conf: Optional[Dict[str, Any]] = None) -> None:
    await _wrapper(conf).publish(subject, payload)


def nats_publish_sync(subject: str, payload: bytes, conf: Optional[Dict[str, Any]] = None) -> None:
    async def run() -> None:
        wrapper = NatsWrapper(conf)
        try:
            await wrapper.publish(subject, payload)
        finally:
            await wrapper.close()

    asyncio.run(run())


async def nats_request(subject: str, payload: bytes, timeout: float = 1.0, conf: Optional[Dict[str, Any]] = None) -> bytes:
    return await _wrapper(conf).request(subject, payload, timeout)


def nats_request_sync(subject: str, payload: bytes, timeout: float = 1.0, conf: Optional[Dict[str, Any]] = None) -> bytes:
    async def run() -> bytes:
        wrapper = NatsWrapper(conf)
        try:
            return await wrapper.request(subject, payload, timeout)
        finally:
            await wrapper.close()

    return asyncio.run(run())


async def drain() -> None:
    await _wrapper().drain()


async def close() -> None:
    loop = asyncio.get_running_loop()
    with _wrappers_lock:
        wrapper = _wrappers.pop(loop, None)
    if wrapper is not None:
        await wrapper.close()


def install_signal_handlers(signums=(signal.SIGINT, signal.SIGTERM)) -> None:
    """Install process handlers that close every live per-loop wrapper safely."""

    def handler(_signum: int, _frame: object) -> None:
        with _wrappers_lock:
            items = list(_wrappers.items())
        for loop, wrapper in items:
            if loop.is_running():
                asyncio.run_coroutine_threadsafe(wrapper.close(), loop)

    for signum in signums:
        signal.signal(signum, handler)
