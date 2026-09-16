from __future__ import annotations

import asyncio
import sys
import threading
import time
from types import ModuleType

from communication.config import KafkaSettings
from communication.transports.kafka import KafkaTransport


class FakeMessage:
    def topic(self):
        return "accounts.user.created"

    def value(self):
        return b'{"id":1}'

    def key(self):
        return b"user-1"

    def headers(self):
        return []

    def partition(self):
        return 0

    def offset(self):
        return 10

    def timestamp(self):
        return (0, 0)

    def error(self):
        return None


class CommitFailingConsumer:
    def __init__(self, config, seeked: threading.Event) -> None:
        self.config = config
        self.seeked = seeked
        self.message = FakeMessage()
        self.polled = False
        self.seeks = []

    def subscribe(self, topics) -> None:
        self.topics = tuple(topics)

    def poll(self, timeout):
        if not self.polled:
            self.polled = True
            return self.message
        time.sleep(min(float(timeout), 0.01))
        return None

    def commit(self, *, message, asynchronous):
        raise RuntimeError("simulated commit failure")

    def seek(self, partition) -> None:
        self.seeks.append(partition)
        self.seeked.set()

    def close(self) -> None:
        return None


def test_kafka_commit_failure_seeks_failed_offset_for_safe_retry(monkeypatch) -> None:
    confluent_kafka = ModuleType("confluent_kafka")

    class TopicPartition:
        def __init__(self, topic, partition, offset) -> None:
            self.topic = topic
            self.partition = partition
            self.offset = offset

    confluent_kafka.TopicPartition = TopicPartition
    monkeypatch.setitem(sys.modules, "confluent_kafka", confluent_kafka)

    seeked = threading.Event()
    consumers = []

    def consumer_factory(config):
        consumer = CommitFailingConsumer(config, seeked)
        consumers.append(consumer)
        return consumer

    async def scenario() -> None:
        transport = KafkaTransport(
            KafkaSettings(bootstrap_servers="kafka:9092"),
            consumer_factory=consumer_factory,
        )

        async def callback(message) -> None:
            await message.ack()

        subscription = await transport.subscribe(
            topics=("accounts.user.created",),
            group_id="accounts",
            callback=callback,
            concurrency=1,
            max_pending=10,
        )
        try:
            assert await asyncio.to_thread(seeked.wait, 2.0)
        finally:
            await subscription.close()

    asyncio.run(scenario())

    assert consumers[0].config["enable.auto.commit"] is False
    assert consumers[0].config["enable.auto.offset.store"] is False
    assert consumers[0].seeks[0].topic == "accounts.user.created"
    assert consumers[0].seeks[0].partition == 0
    assert consumers[0].seeks[0].offset == 10


def test_kafka_health_requires_broker_metadata():
    from types import SimpleNamespace

    class Producer:
        def __init__(self, brokers=None, error=None):
            self.brokers = brokers
            self.error = error

        def list_topics(self, *, timeout):
            assert timeout == 5.0
            if self.error:
                raise self.error
            return SimpleNamespace(brokers=self.brokers)

    async def scenario():
        transport = KafkaTransport(KafkaSettings(bootstrap_servers="kafka:9092"))
        assert not (await transport.health())["healthy"]
        for producer, expected in (
            (Producer(brokers={0: object()}), True),
            (Producer(brokers={}), False),
            (Producer(error=RuntimeError("private connection details")), False),
        ):
            transport._producer = producer
            result = await transport.health()
            assert result == {"transport": "kafka", "healthy": expected, "durable": True, "consumer_worker_failed": False}

    asyncio.run(scenario())
