from __future__ import annotations

import json
import threading
import time
import uuid
from typing import Any, Dict, Optional

from .nat.publisher_high_throughput import PublisherHighThroughput
from .producer import AccountsProducer

_publisher: Optional[PublisherHighThroughput] = None
_publisher_lock = threading.RLock()
_accounts_producer: Optional[AccountsProducer] = None
_accounts_lock = threading.RLock()


def get_publisher(start: bool = False) -> PublisherHighThroughput:
    """Return the legacy NATS-only buffered publisher.

    New application code should use :class:`AccountsProducer` or :class:`EventBus`
    so Kafka, NATS, failover, and mirrored delivery remain configurable.
    """
    global _publisher
    with _publisher_lock:
        if _publisher is None:
            _publisher = PublisherHighThroughput()
    if start:
        raise RuntimeError("Call await get_publisher().start() from an async startup hook")
    return _publisher


def get_accounts_producer() -> AccountsProducer:
    global _accounts_producer
    with _accounts_lock:
        if _accounts_producer is None:
            _accounts_producer = AccountsProducer()
        return _accounts_producer


def _pack_envelope(subject: str, payload: Dict[str, Any], event_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "subject": subject,
        "timestamp": time.time(),
        "payload": payload,
    }


def _to_bytes(obj: Dict[str, Any]) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


async def publish_notification_event_async(
    subject: str,
    payload: Dict[str, Any],
    *,
    event_id: Optional[str] = None,
    use_jetstream: bool = False,
    trace_id: Optional[str] = None,
    publisher: Optional[PublisherHighThroughput] = None,
) -> None:
    if publisher is not None:
        if publisher.running is not True:
            raise RuntimeError("Publisher not started: call await get_publisher().start() at process startup")
        await publisher.publish_async(
            subject=subject,
            payload=_to_bytes(_pack_envelope(subject, payload, event_id)),
            headers={"trace-id": trace_id} if trace_id else None,
            use_jetstream=use_jetstream,
        )
        return
    await get_accounts_producer().publish_event_async(
        payload,
        topic=subject,
        event_type=subject,
        event_id=event_id,
        trace_id=trace_id,
    )


def publish_notification_event_sync(
    subject: str,
    payload: Dict[str, Any],
    *,
    event_id: Optional[str] = None,
    use_jetstream: bool = False,
    trace_id: Optional[str] = None,
    publisher: Optional[PublisherHighThroughput] = None,
) -> None:
    if publisher is not None:
        # Legacy publisher is event-loop bound; callers inside async code must use
        # the async function rather than creating a second loop around it.
        import asyncio

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(
                publish_notification_event_async(
                    subject,
                    payload,
                    event_id=event_id,
                    use_jetstream=use_jetstream,
                    trace_id=trace_id,
                    publisher=publisher,
                )
            )
            return
        raise RuntimeError("publish_notification_event_sync cannot run inside an event loop")
    get_accounts_producer().publish_event(
        payload,
        topic=subject,
        event_type=subject,
        event_id=event_id,
        trace_id=trace_id,
    )
