from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from communication.config import CommunicationSettings
from communication.diagnostics import run_kafka_roundtrip
from communication.protocols import IncomingMessage, TransportPublishResult


class FakeSubscription:
    async def close(self) -> None:
        return None


class FakeTransport:
    def __init__(self, config) -> None:
        self.config = config
        self.callback = None
        self.topic = None

    async def start(self) -> None:
        return None

    async def health(self):
        return {"transport": "kafka", "healthy": True, "durable": True, "consumer_worker_failed": False}

    async def subscribe(self, *, topics, group_id, callback, concurrency, max_pending):
        self.callback = callback
        self.topic = topics[0]
        return FakeSubscription()

    async def publish(self, *, topic, payload, headers, key, timeout):
        assert self.callback is not None
        await self.callback(
            IncomingMessage(
                transport="kafka",
                topic=topic,
                payload=payload,
                headers=headers,
                key=key,
                partition=2,
                offset=41,
                _ack=self._ack,
            )
        )
        return TransportPublishResult(
            transport="kafka",
            topic=topic,
            event_id=headers["event-id"],
            accepted=True,
            partition=2,
            offset=41,
        )

    async def _ack(self) -> None:
        return None

    async def close(self) -> None:
        return None


def test_roundtrip_diagnostic_reports_publish_and_receive(tmp_path: Path) -> None:
    ca = tmp_path / "ca.crt"
    ca.write_text("certificate", encoding="utf-8")
    settings = CommunicationSettings.from_mapping(
        {
            "SERVICE_NAME": "repo-notification",
            "ENABLED_TRANSPORTS": ("kafka",),
            "PRIMARY_TRANSPORT": "kafka",
            "CONSUMER_TRANSPORTS": ("kafka",),
            "KAFKA": {
                "BOOTSTRAP_SERVERS": "kafka:9092",
                "SECURITY_PROTOCOL": "SASL_SSL",
                "SASL_MECHANISM": "SCRAM-SHA-512",
                "USERNAME": "repo-notification",
                "PASSWORD": "secret",
                "SSL_CA_LOCATION": str(ca),
            },
        }
    )

    report = asyncio.run(
        run_kafka_roundtrip(
            settings,
            topic="musttip.communication.diagnostic.v1",
            group_prefix="repo-notification",
            timeout_seconds=5,
            transport_factory=FakeTransport,
        )
    )

    assert report.ok is True
    assert report.stage == "complete"
    assert report.checks == {
        "configuration": True,
        "broker_metadata": True,
        "subscription": True,
        "publish_ack": True,
        "receive": True,
    }
    assert report.publish_partition == 2
    assert report.receive_offset == 41
