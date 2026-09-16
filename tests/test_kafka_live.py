from __future__ import annotations

import asyncio
import os

import pytest

from communication.config import CommunicationSettings
from communication.diagnostics import run_kafka_roundtrip


pytestmark = pytest.mark.integration


def test_real_kafka_publish_consume_roundtrip() -> None:
    """Opt-in live test for the exact Kubernetes/VPS runtime contract.

    Run inside a Kafka-enabled service pod or diagnostic pod with:
      RUN_KAFKA_INTEGRATION=1
      COMMUNICATION_KAFKA_TEST_TOPIC=<pre-provisioned diagnostic topic>

    The current service's normal Kafka environment/Secret mounts are used.
    """
    if os.environ.get("RUN_KAFKA_INTEGRATION") != "1":
        pytest.skip("set RUN_KAFKA_INTEGRATION=1 to execute the live Kafka test")
    topic = os.environ.get("COMMUNICATION_KAFKA_TEST_TOPIC", "").strip()
    if not topic:
        pytest.fail("COMMUNICATION_KAFKA_TEST_TOPIC is required for the live Kafka test")

    settings = CommunicationSettings.from_environment()
    report = asyncio.run(
        run_kafka_roundtrip(
            settings,
            topic=topic,
            group_prefix=os.environ.get("COMMUNICATION_KAFKA_TEST_GROUP_PREFIX"),
            timeout_seconds=float(os.environ.get("COMMUNICATION_KAFKA_TEST_TIMEOUT", "30")),
        )
    )

    assert report.ok is True
    assert report.checks["broker_metadata"] is True
    assert report.checks["publish_ack"] is True
    assert report.checks["receive"] is True
    assert report.publish_partition == report.receive_partition
    assert report.publish_offset == report.receive_offset
