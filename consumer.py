from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import json
import logging
import random
import signal
import threading
import uuid
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

from .bus import EventBus
from .config import CommunicationSettings
from .envelope import EventEnvelope, verify_payload_signature
from .handlers import EventContext, HandlerRegistry
from .idempotency import ClaimState, IdempotencyStore, build_idempotency_store
from .protocols import IncomingMessage

logger = logging.getLogger(__name__)
Handler = Callable[[Any, Dict[str, Any]], object]
Deserializer = Callable[[Union[bytes, str], Any], Any]


def _deserialize_default(raw: Union[bytes, str], _meta: Any) -> Any:
    encoded = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
    try:
        return json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return encoded


class ConsumerService:
    """Backward-compatible consumer service backed by one or more transports."""

    def __init__(
        self,
        topics: Sequence[str],
        group_id: str,
        *,
        consumer_conf: Optional[Dict[str, Any]] = None,
        deserializer: Optional[Deserializer] = None,
        handler: Optional[Handler] = None,
        concurrency: int = 1,
        commit_on_success: bool = True,
        dlq_suffix: str = ".dlq",
        settings_obj: CommunicationSettings | None = None,
        event_bus: EventBus | None = None,
        handler_registry: HandlerRegistry | None = None,
        idempotency_store: IdempotencyStore | None = None,
        transports: Sequence[str] | None = None,
    ) -> None:
        if not topics:
            raise ValueError("at least one consumer topic is required")
        if not isinstance(group_id, str) or not group_id.strip():
            raise ValueError("group_id must not be empty")
        self.settings = settings_obj or CommunicationSettings.from_django_settings()
        self.topics = tuple(self.settings.canonical_topic(topic) for topic in topics)
        self.group_id = group_id.strip()
        self.consumer_conf = dict(consumer_conf or {})
        self.deserializer = deserializer or _deserialize_default
        self.handler = handler
        self.handler_registry = handler_registry
        self.concurrency = max(1, int(concurrency))
        self.commit_on_success = bool(commit_on_success)
        self.dlq_suffix = str(dlq_suffix or self.settings.dlq_suffix)
        self.transports = tuple(transports or self.settings.consumer_transports)
        self._event_bus = event_bus
        idempotency_backend = self.consumer_conf.get("idempotency_backend")
        if idempotency_backend is None:
            try:
                from django.conf import settings as django_settings

                idempotency_backend = getattr(
                    django_settings,
                    "COMMUNICATION_IDEMPOTENCY_BACKEND",
                    "django-cache",
                )
            except Exception:
                idempotency_backend = "django-cache"
        self._idempotency = idempotency_store or build_idempotency_store(
            backend=str(idempotency_backend),
            fail_closed=self.settings.idempotency_fail_closed,
            consumer_group=self.group_id,
        )
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = threading.Event()
        self._started_event = threading.Event()
        self._startup_error: BaseException | None = None
        self._subscription: Any = None
        self._pending: set[asyncio.Task[Any]] = set()
        self._max_pending = self.settings.consumer_max_pending

    def install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("signal handlers can only be installed from the main thread")

        def stop_handler(_signum: int, _frame: object) -> None:
            self.stop(timeout=self.settings.graceful_shutdown_seconds)

        signal.signal(signal.SIGINT, stop_handler)
        signal.signal(signal.SIGTERM, stop_handler)

    def start(self, handler: Handler | None = None, max_pending_tasks: Optional[int] = None) -> threading.Thread:
        if self._thread and self._thread.is_alive():
            return self._thread
        if handler is not None:
            self.handler = handler
        if self.handler is None and self.handler_registry is None:
            raise RuntimeError("ConsumerService.start requires a handler or HandlerRegistry")
        self._max_pending = max(1, int(max_pending_tasks or self.settings.consumer_max_pending))
        self._stop_event.clear()
        self._started_event.clear()
        self._startup_error = None
        self._thread = threading.Thread(target=self._thread_main, name=f"communication-consumer-{self.group_id}", daemon=True)
        self._thread.start()
        if not self._started_event.wait(timeout=15.0):
            raise RuntimeError("consumer did not start within the startup timeout")
        if self._startup_error is not None:
            raise RuntimeError("consumer failed to start") from self._startup_error
        return self._thread

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._main())
        except BaseException as exc:
            self._startup_error = exc
            self._started_event.set()
            logger.exception("consumer runtime failed")
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            self._loop = None

    async def _main(self) -> None:
        bus = self._event_bus or EventBus(self.settings)
        self._event_bus = bus
        self._subscription = await bus.subscribe(
            topics=self.topics,
            group_id=self.group_id,
            callback=self._enqueue_message,
            transports=self.transports,
            concurrency=self.concurrency,
            max_pending=self._max_pending,
        )
        self._started_event.set()
        while not self._stop_event.is_set():
            await asyncio.sleep(0.1)
        if self._subscription is not None:
            await self._subscription.close()
        if self._pending:
            done, pending = await asyncio.wait(
                self._pending,
                timeout=self.settings.graceful_shutdown_seconds,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        await bus.close()

    async def _enqueue_message(self, message: IncomingMessage) -> None:
        while len(self._pending) >= self._max_pending:
            done, _ = await asyncio.wait(self._pending, return_when=asyncio.FIRST_COMPLETED)
            self._pending.difference_update(done)
        task = asyncio.create_task(self._process_message(message))
        self._pending.add(task)
        try:
            # Transport adapters commit/ack only after this callback returns, so
            # processing must finish before the callback completes.
            await task
        finally:
            self._pending.discard(task)

    async def _legacy_event(self, message: IncomingMessage) -> EventEnvelope:
        decoded = self.deserializer(message.payload, {"topic": message.topic, "headers": message.headers})
        event_id = message.headers.get("event-id")
        try:
            uuid.UUID(str(event_id))
        except (ValueError, TypeError, AttributeError):
            # Kafka offsets and JetStream stream sequences remain stable across
            # redelivery while allowing two legitimate messages with identical
            # payloads to be processed independently. Core NATS has no durable
            # delivery identity, so generate a new event id rather than dropping
            # equal payloads as false duplicates.
            if message.partition is not None and message.offset is not None:
                identity = (
                    f"{message.transport}:{message.topic}:"
                    f"{message.partition}:{message.offset}"
                )
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
            elif message.sequence is not None:
                identity = f"{message.transport}:{message.topic}:{message.sequence}"
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, identity))
            else:
                event_id = str(uuid.uuid4())
        return EventEnvelope(
            event_id=str(event_id),
            event_type=message.headers.get("event-type") or message.topic,
            schema_version=message.headers.get("event-version") or self.settings.schema_version,
            source=message.headers.get("event-source") or "legacy",
            topic=message.topic,
            trace_id=message.headers.get("trace-id"),
            correlation_id=message.headers.get("correlation-id"),
            causation_id=message.headers.get("causation-id"),
            tenant_id=message.headers.get("tenant-id"),
            data=decoded,
        )

    async def _decode_event(self, message: IncomingMessage) -> EventEnvelope:
        verify_payload_signature(
            message.payload,
            message.headers,
            secrets=self.settings.signing_keys,
            required=self.settings.signing_required,
        )
        content_type = str(message.headers.get("content-type", ""))
        if content_type == "application/vnd.musttip.event+json":
            return EventEnvelope.from_bytes(message.payload, maximum_bytes=self.settings.max_event_bytes)
        try:
            return EventEnvelope.from_bytes(message.payload, maximum_bytes=self.settings.max_event_bytes)
        except Exception:
            return await self._legacy_event(message)

    async def _call_handler(self, event: EventEnvelope, context: EventContext) -> None:
        if self.handler_registry is not None:
            await self.handler_registry.dispatch(event, context)
            return
        assert self.handler is not None
        meta = {
            "topic": context.topic,
            "subject": context.topic,
            "transport": context.transport,
            "headers": dict(context.headers),
            "key": context.key,
            "partition": context.partition,
            "offset": context.offset,
            "sequence": context.sequence,
            "delivery_count": context.delivery_count,
            "event_id": event.event_id,
            "event_type": event.event_type,
            "trace_id": event.trace_id,
        }
        if inspect.iscoroutinefunction(self.handler):
            result = await self.handler(event.data, meta)
        else:
            result = await asyncio.to_thread(self.handler, event.data, meta)
            if inspect.isawaitable(result):
                result = await result
        if result is False:
            raise RuntimeError("event handler returned False")

    async def _publish_dlq(self, message: IncomingMessage, event: EventEnvelope | None, error: BaseException) -> bool:
        if message.topic.endswith(self.dlq_suffix):
            return False
        headers = dict(message.headers)
        headers["x-dlq-reason"] = type(error).__name__[:128]
        headers["x-original-transport"] = message.transport
        headers["x-original-topic"] = message.topic
        headers["x-delivery-count"] = str(message.delivery_count)
        event_id = event.event_id if event is not None else headers.get("event-id") or str(uuid.uuid4())
        try:
            assert self._event_bus is not None
            await self._event_bus.publish_raw(
                topic=f"{message.topic}{self.dlq_suffix}",
                payload=message.payload,
                headers=headers,
                event_id=event_id,
                event=event,
                mode="primary",
                transports=(message.transport,),
                key=message.key or event_id,
            )
            return True
        except Exception:
            logger.exception("DLQ publication failed", extra={"transport": message.transport, "topic": message.topic})
            return False

    async def _process_message(self, message: IncomingMessage) -> None:
        event: EventEnvelope | None = None
        claim_key: str | None = None
        try:
            event = await self._decode_event(message)
        except Exception as exc:
            logger.exception(
                "event decoding failed",
                extra={"transport": message.transport, "topic": message.topic},
            )
            if await self._publish_dlq(message, event, exc):
                await message.term()
            else:
                await message.nack(self.settings.retry_backoff_seconds)
            return

        try:
            claim_key = f"{self.group_id}:{event.event_id}"
            claim = await self._idempotency.claim(claim_key, self.settings.idempotency_ttl_seconds)
            if claim is ClaimState.COMPLETE:
                if self.commit_on_success:
                    await message.ack()
                else:
                    await message.nack(0.0)
                return
            if claim is ClaimState.PROCESSING:
                await message.nack(min(5.0, self.settings.retry_max_seconds))
                return
        except Exception:
            logger.exception(
                "idempotency claim failed",
                extra={"transport": message.transport, "topic": message.topic},
            )
            await message.nack(self.settings.retry_backoff_seconds)
            return

        context = EventContext(
            transport=message.transport,
            topic=message.topic,
            headers=message.headers,
            key=message.key,
            partition=message.partition,
            offset=message.offset,
            sequence=message.sequence,
            delivery_count=message.delivery_count,
        )
        last_error: BaseException | None = None
        handled = False
        for attempt in range(self.settings.handler_retries + 1):
            try:
                await self._call_handler(event, context)
            except Exception as exc:
                last_error = exc
                if attempt < self.settings.handler_retries:
                    delay = min(
                        self.settings.retry_max_seconds,
                        self.settings.retry_backoff_seconds * (2**attempt)
                        + random.random() * 0.25,
                    )
                    await asyncio.sleep(delay)
            else:
                handled = True
                break
        if handled:
            if self.commit_on_success:
                try:
                    await self._idempotency.complete(
                        claim_key,
                        self.settings.idempotency_ttl_seconds,
                    )
                except Exception:
                    # The side effect may already have succeeded. Leave the
                    # broker record uncommitted and release the failed claim
                    # so an idempotent handler can safely retry.
                    logger.exception(
                        "idempotency completion failed",
                        extra={
                            "transport": message.transport,
                            "topic": message.topic,
                        },
                    )
                    await self._idempotency.release(claim_key)
                    await message.nack(self.settings.retry_backoff_seconds)
                    return
                # Do not turn an acknowledgement/commit failure into a DLQ
                # event. The completed inbox claim makes broker redelivery safe
                # and the next attempt can commit without rehandling.
                await message.ack()
            else:
                await self._idempotency.release(claim_key)
                await message.nack(0.0)
            return
        assert last_error is not None
        if await self._publish_dlq(message, event, last_error):
            try:
                await self._idempotency.complete(
                    claim_key,
                    self.settings.idempotency_ttl_seconds,
                )
            except Exception:
                # DLQ delivery is the durable terminal record. A failed inbox
                # update must not cause the poison event to block the partition.
                logger.exception(
                    "idempotency completion failed after DLQ publication",
                    extra={"transport": message.transport, "topic": message.topic},
                )
            await message.term()
        else:
            try:
                await self._idempotency.release(claim_key)
            finally:
                await message.nack(self.settings.retry_backoff_seconds)

    def stop(self, timeout: float = 10.0) -> None:
        self._stop_event.set()
        loop = self._loop
        if loop and loop.is_running():
            loop.call_soon_threadsafe(lambda: None)
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(0.1, timeout))
        if thread and thread.is_alive():
            logger.warning("consumer did not stop within timeout", extra={"group_id": self.group_id})


def create_consumer_service(
    topics: Sequence[str],
    group_id: str,
    *,
    consumer_conf: Optional[Dict[str, Any]] = None,
    deserializer: Optional[Deserializer] = None,
    handler: Optional[Handler] = None,
    concurrency: int = 1,
    commit_on_success: bool = True,
    dlq_suffix: str = ".dlq",
    **kwargs: Any,
) -> ConsumerService:
    return ConsumerService(
        topics,
        group_id,
        consumer_conf=consumer_conf,
        deserializer=deserializer,
        handler=handler,
        concurrency=concurrency,
        commit_on_success=commit_on_success,
        dlq_suffix=dlq_suffix,
        **kwargs,
    )


def start_consumer_in_background(
    topics: Sequence[str],
    group_id: str,
    *,
    handler: Handler,
    consumer_conf: Optional[Dict[str, Any]] = None,
    deserializer: Optional[Deserializer] = None,
    concurrency: int = 1,
    commit_on_success: bool = True,
    dlq_suffix: str = ".dlq",
    install_signal_handlers: bool = False,
    max_pending_tasks: Optional[int] = None,
    **kwargs: Any,
) -> Tuple[ConsumerService, threading.Thread]:
    consumer = create_consumer_service(
        topics,
        group_id,
        consumer_conf=consumer_conf,
        deserializer=deserializer,
        handler=handler,
        concurrency=concurrency,
        commit_on_success=commit_on_success,
        dlq_suffix=dlq_suffix,
        **kwargs,
    )
    if install_signal_handlers:
        consumer.install_signal_handlers()
    return consumer, consumer.start(handler, max_pending_tasks=max_pending_tasks)


def stop_consumer(consumer: ConsumerService, timeout: float = 10.0) -> None:
    consumer.stop(timeout)


def run_consumer_once(
    topics: Sequence[str],
    group_id: str,
    *,
    user_handler: Handler,
    consumer_conf: Optional[Dict[str, Any]] = None,
    deserializer: Optional[Deserializer] = None,
    concurrency: int = 1,
    commit_on_success: bool = True,
    dlq_suffix: str = ".dlq",
    timeout: float = 10.0,
    **kwargs: Any,
) -> Optional[Tuple[Any, Dict[str, Any]]]:
    done = threading.Event()
    result: dict[str, Any] = {}

    def handler(payload: Any, meta: Dict[str, Any]) -> bool:
        accepted = bool(user_handler(payload, meta))
        if accepted and not done.is_set():
            result.update(payload=payload, meta=meta)
            done.set()
        return accepted

    consumer = create_consumer_service(
        topics,
        group_id,
        consumer_conf=consumer_conf,
        deserializer=deserializer,
        handler=handler,
        concurrency=concurrency,
        commit_on_success=commit_on_success,
        dlq_suffix=dlq_suffix,
        **kwargs,
    )
    thread = consumer.start(handler)
    try:
        if not done.wait(timeout=max(0.1, timeout)):
            return None
        return result["payload"], result["meta"]
    finally:
        consumer.stop(timeout=5.0)
        thread.join(timeout=2.0)


async def run_consumer_once_async(*args: Any, **kwargs: Any) -> Optional[Tuple[Any, Dict[str, Any]]]:
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(run_consumer_once, *args, **kwargs))


@contextlib.contextmanager
def consumer_context(
    topics: Sequence[str],
    group_id: str,
    *,
    handler: Handler,
    max_pending_tasks: Optional[int] = None,
    **kwargs: Any,
):
    consumer = create_consumer_service(topics, group_id, handler=handler, **kwargs)
    thread = consumer.start(handler, max_pending_tasks=max_pending_tasks)
    try:
        yield consumer
    finally:
        consumer.stop(timeout=5.0)
        thread.join(timeout=2.0)
