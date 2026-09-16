from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from ..config import KafkaSettings
from ..errors import PublishOutcomeUnknown, TransportUnavailable
from ..protocols import IncomingMessage, MessageCallback, TransportPublishResult

logger = logging.getLogger(__name__)


def _decode_headers(values: object) -> dict[str, str]:
    output: dict[str, str] = {}
    if not values:
        return output
    for raw_name, raw_value in values:
        name = str(raw_name).strip().casefold()
        if raw_value is None:
            continue
        if isinstance(raw_value, bytes):
            value = raw_value.decode("utf-8", "replace")
        else:
            value = str(raw_value)
        output[name] = value
    return output


@dataclass(slots=True)
class _KafkaSubscription:
    stop_event: threading.Event
    threads: list[threading.Thread]
    consumers: list[Any]
    closed: bool = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.stop_event.set()
        # Each Consumer is closed by the same worker thread that owns it.
        # confluent-kafka Consumer instances must not be closed concurrently
        # from the asyncio thread while poll() is active.
        for thread in self.threads:
            await asyncio.to_thread(thread.join, 5.0)


class KafkaTransport:
    name = "kafka"
    durable = True

    def __init__(self, config: KafkaSettings, *, producer_factory: Any = None, consumer_factory: Any = None) -> None:
        self.config = config
        self._producer_factory = producer_factory
        self._consumer_factory = consumer_factory
        self._producer: Any = None
        self._producer_lock = threading.RLock()
        self._poll_stop = threading.Event()
        self._poll_thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._consumer_failure: BaseException | None = None
        self._consumer_failure_lock = threading.Lock()

    @staticmethod
    def _libraries() -> tuple[Any, Any, Any]:
        try:
            from confluent_kafka import Consumer, KafkaException, Producer
        except ImportError as exc:
            raise TransportUnavailable("confluent-kafka is required for the Kafka transport") from exc
        return Producer, Consumer, KafkaException

    async def start(self) -> None:
        if self._producer is not None:
            return
        factory = self._producer_factory
        if factory is None:
            Producer, _, _ = self._libraries()
            factory = Producer
        self._loop = asyncio.get_running_loop()
        try:
            self._producer = factory(self.config.producer_config())
        except Exception as exc:
            raise TransportUnavailable("Kafka producer initialization failed") from exc
        self._poll_stop.clear()

        def poll_loop() -> None:
            while not self._poll_stop.is_set():
                try:
                    self._producer.poll(0.1)
                except Exception:
                    logger.exception("Kafka producer poll failed")
                    time.sleep(0.5)

        self._poll_thread = threading.Thread(target=poll_loop, name="communication-kafka-producer", daemon=True)
        self._poll_thread.start()

    async def close(self) -> None:
        producer = self._producer
        self._poll_stop.set()
        if self._poll_thread is not None:
            await asyncio.to_thread(self._poll_thread.join, 5.0)
            self._poll_thread = None
        if producer is not None:
            try:
                remaining = await asyncio.to_thread(producer.flush, 10.0)
                if remaining:
                    logger.warning(
                        "Kafka producer closed with undelivered messages",
                        extra={"remaining": int(remaining)},
                    )
            except Exception:
                logger.warning("Kafka producer flush failed during shutdown", exc_info=True)
        self._producer = None

    async def flush(self, timeout: float = 10.0) -> None:
        if self._producer is None:
            return
        remaining = await asyncio.to_thread(self._producer.flush, timeout)
        if remaining:
            raise TransportUnavailable(f"Kafka producer still has {remaining} undelivered messages")

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
        if self._producer is None:
            raise TransportUnavailable("Kafka producer is unavailable")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[TransportPublishResult] = loop.create_future()
        event_id = str(headers.get("event-id", ""))

        def delivery(error: Any, message: Any) -> None:
            if future.done():
                return
            if error is not None:
                loop.call_soon_threadsafe(future.set_exception, TransportUnavailable("Kafka delivery failed"))
                return
            timestamp_ms = None
            try:
                _, timestamp_ms = message.timestamp()
            except Exception:
                timestamp_ms = None
            result = TransportPublishResult(
                transport=self.name,
                topic=str(message.topic()),
                event_id=event_id,
                accepted=True,
                partition=int(message.partition()),
                offset=int(message.offset()),
                timestamp=(datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc) if timestamp_ms else datetime.now(timezone.utc)),
            )
            loop.call_soon_threadsafe(future.set_result, result)

        encoded_headers = [(name, value.encode("utf-8")) for name, value in headers.items()]
        deadline = loop.time() + timeout
        while True:
            try:
                with self._producer_lock:
                    self._producer.produce(
                        topic=topic,
                        value=payload,
                        key=key.encode("utf-8") if key else None,
                        headers=encoded_headers,
                        on_delivery=delivery,
                    )
                break
            except BufferError as exc:
                if loop.time() >= deadline:
                    raise TransportUnavailable("Kafka producer queue is full") from exc
                self._producer.poll(0)
                await asyncio.sleep(0.01)
            except Exception as exc:
                raise TransportUnavailable("Kafka produce failed") from exc
        try:
            return await asyncio.wait_for(future, timeout=max(0.01, deadline - loop.time()))
        except asyncio.TimeoutError as exc:
            raise PublishOutcomeUnknown(
                "Kafka delivery acknowledgement timed out; delivery outcome is unknown"
            ) from exc

    async def subscribe(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        callback: MessageCallback,
        concurrency: int,
        max_pending: int,
    ) -> _KafkaSubscription:
        factory = self._consumer_factory
        if factory is None:
            _, Consumer, _ = self._libraries()
            factory = Consumer
        loop = asyncio.get_running_loop()
        stop_event = threading.Event()
        threads: list[threading.Thread] = []
        consumers: list[Any] = []
        startup: queue.Queue[BaseException | None] = queue.Queue()
        worker_count = max(1, int(concurrency))
        with self._consumer_failure_lock:
            self._consumer_failure = None

        def worker(index: int) -> None:
            consumer = None
            try:
                consumer = factory(self.config.consumer_config(group_id))
                consumers.append(consumer)
                consumer.subscribe(list(topics))
                startup.put(None)
                while not stop_event.is_set():
                    message = consumer.poll(0.5)
                    if message is None:
                        continue
                    if message.error():
                        logger.warning("Kafka consumer error: %s", message.error())
                        continue
                    raw_headers = _decode_headers(message.headers())
                    key_bytes = message.key()
                    key = key_bytes.decode("utf-8", "replace") if isinstance(key_bytes, bytes) else (str(key_bytes) if key_bytes else None)
                    timestamp_ms = None
                    try:
                        _, timestamp_ms = message.timestamp()
                    except Exception:
                        timestamp_ms = None
                    decision: dict[str, object] = {"ack": False, "nack_delay": None, "term": False}

                    async def ack() -> None:
                        decision["ack"] = True

                    async def nack(delay_seconds: float | None = None) -> None:
                        decision["nack_delay"] = delay_seconds or 0.0

                    async def term() -> None:
                        decision["term"] = True

                    incoming = IncomingMessage(
                        transport=self.name,
                        topic=str(message.topic()),
                        payload=bytes(message.value() or b""),
                        headers=raw_headers,
                        key=key,
                        partition=int(message.partition()),
                        offset=int(message.offset()),
                        timestamp=(datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc) if timestamp_ms else None),
                        delivery_count=1,
                        raw_message=message,
                        _ack=ack,
                        _nack=nack,
                        _term=term,
                    )
                    future = asyncio.run_coroutine_threadsafe(callback(incoming), loop)
                    try:
                        future.result(timeout=float(self.config.consumer_callback_timeout_seconds))
                    except concurrent.futures.TimeoutError:
                        future.cancel()
                        logger.error(
                            "Kafka message callback exceeded configured timeout",
                            extra={"worker": index, "topic": str(message.topic())},
                        )
                        decision["nack_delay"] = 0.0
                    except Exception:
                        logger.exception("Kafka message callback failed")
                        decision["nack_delay"] = 0.0
                    committed = False
                    if decision["ack"] or decision["term"]:
                        try:
                            consumer.commit(message=message, asynchronous=False)
                            committed = True
                        except Exception:
                            logger.exception("Kafka offset commit failed")
                    if not committed:
                        # Do not commit. Seek to the failed offset so the same consumer
                        # retries after an optional bounded delay. Commit failures
                        # also seek, allowing a completed inbox claim to safely retry
                        # the offset commit instead of advancing the local position.
                        delay = float(decision["nack_delay"] or 0.0)
                        if delay > 0:
                            time.sleep(min(delay, 30.0))
                        try:
                            from confluent_kafka import TopicPartition

                            consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
                        except Exception:
                            logger.exception("Kafka seek after handler failure failed")
            except BaseException as exc:
                if startup.empty():
                    startup.put(exc)
                with self._consumer_failure_lock:
                    self._consumer_failure = exc
                logger.exception("Kafka consumer worker failed", extra={"worker": index})
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

        for index in range(worker_count):
            thread = threading.Thread(target=worker, args=(index,), name=f"communication-kafka-consumer-{index}", daemon=True)
            threads.append(thread)
            thread.start()
        try:
            for _ in threads:
                result = await asyncio.to_thread(startup.get, True, 10.0)
                if result is not None:
                    raise TransportUnavailable("Kafka consumer failed to start") from result
        except (queue.Empty, TransportUnavailable) as exc:
            stop_event.set()
            await asyncio.gather(
                *(asyncio.to_thread(thread.join, 5.0) for thread in threads),
                return_exceptions=True,
            )
            if isinstance(exc, TransportUnavailable):
                raise
            raise TransportUnavailable("Kafka consumer startup timed out") from exc
        return _KafkaSubscription(stop_event, threads, consumers)

    async def health(self) -> Mapping[str, object]:
        producer = self._producer
        healthy = False
        if producer is not None:
            try:
                # Construction is lazy: only metadata verifies DNS, TLS, SASL,
                # and broker reachability. Do not create or probe a topic here.
                metadata = await asyncio.to_thread(producer.list_topics, timeout=5.0)
                healthy = bool(metadata.brokers)
            except Exception:
                # Client errors can contain connection details; keep diagnostics
                # stable and free of credentials.
                healthy = False
        with self._consumer_failure_lock:
            consumer_failed = self._consumer_failure is not None
        return {
            "transport": self.name,
            "healthy": healthy and not consumer_failed,
            "durable": self.durable,
            "consumer_worker_failed": consumer_failed,
        }
