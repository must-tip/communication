from __future__ import annotations

import pytest

from communication.nat.publisher_high_throughput import PublisherHighThroughput
from communication.protocols import TransportPublishResult


class FakeTransport:
    def __init__(self):
        self.items = []

    async def start(self):
        return None

    async def publish(self, *, topic, payload, headers, key, timeout):
        self.items.append((topic, payload))
        return TransportPublishResult("nats", topic, headers["event-id"], True)

    async def flush(self, timeout):
        return None

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_buffered_publisher_drains_before_stop() -> None:
    publisher = PublisherHighThroughput(worker_count=2, queue_maxsize=100)
    transport = FakeTransport()
    publisher._transport = transport
    await publisher.start()
    for index in range(25):
        await publisher.publish_async(subject="accounts.events", payload=str(index).encode())
    await publisher.stop()
    assert len(transport.items) == 25
    assert publisher.metrics["published"] == 25
