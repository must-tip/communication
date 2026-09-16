from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable, Mapping

from .config import CommunicationSettings
from .errors import CommunicationError, ConfigurationError
from .protocols import IncomingMessage
from .transports.kafka import KafkaTransport

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class KafkaDiagnosticReport:
    ok: bool
    stage: str
    service_name: str
    topic: str | None
    group_id: str | None
    elapsed_seconds: float
    checks: Mapping[str, bool] = field(default_factory=dict)
    publish_partition: int | None = None
    publish_offset: int | None = None
    receive_partition: int | None = None
    receive_offset: int | None = None
    error_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class KafkaDiagnosticError(CommunicationError):
    def __init__(self, report: KafkaDiagnosticReport) -> None:
        super().__init__(f"Kafka diagnostic failed at stage: {report.stage}")
        self.report = report


def _validate_diagnostic_topic(topic: str) -> str:
    value = str(topic).strip()
    if not value:
        raise ConfigurationError(
            "A dedicated diagnostic topic is required. Set COMMUNICATION_KAFKA_TEST_TOPIC "
            "or pass --topic. The SDK will not test against a business topic implicitly."
        )
    if len(value) > 249 or any(character.isspace() for character in value):
        raise ConfigurationError("The Kafka diagnostic topic is invalid")
    return value


def _validate_runtime_files(settings: CommunicationSettings) -> None:
    kafka = settings.kafka
    if kafka.security_protocol in {"SSL", "SASL_SSL"}:
        if not kafka.ssl_ca_location:
            raise ConfigurationError("Kafka TLS requires a CA file")
        ca_path = Path(kafka.ssl_ca_location)
        if not ca_path.is_file():
            raise ConfigurationError("Kafka CA file does not exist or is not a regular file")
        try:
            if ca_path.stat().st_size <= 0:
                raise ConfigurationError("Kafka CA file is empty")
        except OSError as exc:
            raise ConfigurationError("Kafka CA file cannot be inspected") from exc


async def run_kafka_roundtrip(
    settings: CommunicationSettings,
    *,
    topic: str,
    group_prefix: str | None = None,
    timeout_seconds: float = 30.0,
    transport_factory: Callable[..., KafkaTransport] = KafkaTransport,
) -> KafkaDiagnosticReport:
    """Verify configuration, broker metadata, publish acknowledgement and consumption.

    The topic must already exist and the supplied principal must have Describe/Write
    access to it plus Read access to a consumer-group prefix. Automatic topic creation
    remains disabled. No business EventEnvelope is emitted; the payload is a diagnostic
    marker intended for a dedicated operational test topic.
    """
    started = time.monotonic()
    selected_topic = _validate_diagnostic_topic(topic)
    timeout = float(timeout_seconds)
    if not 1.0 <= timeout <= 300.0:
        raise ConfigurationError("Kafka diagnostic timeout must be between 1 and 300 seconds")
    if "kafka" not in settings.enabled_transports and "kafka" not in settings.consumer_transports:
        raise ConfigurationError("Kafka is not enabled in communication settings")
    _validate_runtime_files(settings)

    safe_prefix = str(group_prefix or settings.service_name).strip()
    if not safe_prefix or len(safe_prefix) > 180 or any(ch.isspace() for ch in safe_prefix):
        raise ConfigurationError("Kafka diagnostic group prefix is invalid")
    run_id = uuid.uuid4().hex
    group_id = f"{safe_prefix}-sdk-check-{run_id[:12]}"
    marker = f"musttip-communication:{settings.service_name}:{run_id}"
    payload = json.dumps(
        {"type": "musttip.communication.diagnostic", "marker": marker},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    checks: dict[str, bool] = {
        "configuration": True,
        "broker_metadata": False,
        "subscription": False,
        "publish_ack": False,
        "receive": False,
    }
    transport = transport_factory(settings.kafka)
    subscription = None
    received = asyncio.Event()
    receive_position: tuple[int | None, int | None] = (None, None)

    async def callback(message: IncomingMessage) -> None:
        nonlocal receive_position
        try:
            decoded = json.loads(message.payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            await message.ack()
            return
        if isinstance(decoded, dict) and decoded.get("marker") == marker:
            receive_position = (message.partition, message.offset)
            received.set()
        # The consumer group is unique to this diagnostic run, so acknowledging
        # unrelated older records only advances this disposable group's offsets.
        await message.ack()

    stage = "initialization"
    try:
        stage = "producer_start"
        await transport.start()

        stage = "broker_metadata"
        health = await transport.health()
        if not bool(health.get("healthy")):
            raise KafkaDiagnosticError(
                KafkaDiagnosticReport(
                    ok=False,
                    stage=stage,
                    service_name=settings.service_name,
                    topic=selected_topic,
                    group_id=group_id,
                    elapsed_seconds=round(time.monotonic() - started, 3),
                    checks=checks,
                    error_type="BrokerMetadataUnavailable",
                )
            )
        checks["broker_metadata"] = True

        stage = "subscription"
        subscription = await transport.subscribe(
            topics=(selected_topic,),
            group_id=group_id,
            callback=callback,
            concurrency=1,
            max_pending=1,
        )
        checks["subscription"] = True

        # Give the consumer a short opportunity to join the group before publish.
        await asyncio.sleep(0.5)

        stage = "publish_ack"
        publish_result = await transport.publish(
            topic=selected_topic,
            payload=payload,
            headers={
                "content-type": "application/json",
                "event-id": run_id,
                "x-musttip-diagnostic": "1",
            },
            key=run_id,
            timeout=min(timeout, settings.publish_timeout_seconds),
        )
        checks["publish_ack"] = bool(publish_result.accepted)
        if not checks["publish_ack"]:
            raise KafkaDiagnosticError(
                KafkaDiagnosticReport(
                    ok=False,
                    stage=stage,
                    service_name=settings.service_name,
                    topic=selected_topic,
                    group_id=group_id,
                    elapsed_seconds=round(time.monotonic() - started, 3),
                    checks=checks,
                    error_type="PublishNotAccepted",
                )
            )

        stage = "receive"
        remaining = max(0.1, timeout - (time.monotonic() - started))
        await asyncio.wait_for(received.wait(), timeout=remaining)
        checks["receive"] = True
        return KafkaDiagnosticReport(
            ok=True,
            stage="complete",
            service_name=settings.service_name,
            topic=selected_topic,
            group_id=group_id,
            elapsed_seconds=round(time.monotonic() - started, 3),
            checks=checks,
            publish_partition=publish_result.partition,
            publish_offset=publish_result.offset,
            receive_partition=receive_position[0],
            receive_offset=receive_position[1],
        )
    except KafkaDiagnosticError:
        raise
    except asyncio.TimeoutError as exc:
        report = KafkaDiagnosticReport(
            ok=False,
            stage=stage,
            service_name=settings.service_name,
            topic=selected_topic,
            group_id=group_id,
            elapsed_seconds=round(time.monotonic() - started, 3),
            checks=checks,
            error_type="TimeoutError",
        )
        raise KafkaDiagnosticError(report) from exc
    except Exception as exc:
        report = KafkaDiagnosticReport(
            ok=False,
            stage=stage,
            service_name=settings.service_name,
            topic=selected_topic,
            group_id=group_id,
            elapsed_seconds=round(time.monotonic() - started, 3),
            checks=checks,
            error_type=type(exc).__name__,
        )
        raise KafkaDiagnosticError(report) from exc
    finally:
        if subscription is not None:
            try:
                await subscription.close()
            except Exception:
                logger.exception("Kafka diagnostic subscription shutdown failed")
        try:
            await transport.close()
        except Exception:
            logger.exception("Kafka diagnostic transport shutdown failed")


def _load_settings(source: str) -> CommunicationSettings:
    if source == "environment":
        return CommunicationSettings.from_environment()
    if source == "django":
        try:
            import django
        except ImportError as exc:
            raise ConfigurationError("Django is required for --source=django") from exc
        if not os.environ.get("DJANGO_SETTINGS_MODULE"):
            raise ConfigurationError("DJANGO_SETTINGS_MODULE is required for --source=django")
        django.setup()
        return CommunicationSettings.from_django_settings()
    raise ConfigurationError("diagnostic settings source must be environment or django")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="musttip-communication-kafka-check",
        description="Validate MustTip Kafka configuration and perform a real publish/consume round trip.",
    )
    parser.add_argument(
        "--source",
        choices=("environment", "django"),
        default=os.environ.get("COMMUNICATION_DIAGNOSTIC_SOURCE", "environment"),
        help="Load directly from cloud-services environment variables or from Django settings.",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("COMMUNICATION_KAFKA_TEST_TOPIC"),
        help="Pre-provisioned dedicated diagnostic topic. Required for round-trip testing.",
    )
    parser.add_argument(
        "--group-prefix",
        default=os.environ.get("COMMUNICATION_KAFKA_TEST_GROUP_PREFIX"),
        help="Consumer-group prefix authorized by the service ACL. Defaults to the service name.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("COMMUNICATION_KAFKA_TEST_TIMEOUT", "30")),
    )
    parser.add_argument("--debug", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )
    try:
        settings = _load_settings(args.source)
        report = asyncio.run(
            run_kafka_roundtrip(
                settings,
                topic=args.topic,
                group_prefix=args.group_prefix,
                timeout_seconds=args.timeout,
            )
        )
    except KafkaDiagnosticError as exc:
        print(json.dumps(exc.report.to_dict(), sort_keys=True), file=sys.stderr)
        if args.debug:
            logger.exception("Kafka diagnostic failed")
        return 2
    except Exception as exc:
        report = KafkaDiagnosticReport(
            ok=False,
            stage="configuration",
            service_name=os.environ.get("SERVICE_NAME", "unknown"),
            topic=args.topic,
            group_id=None,
            elapsed_seconds=0.0,
            checks={"configuration": False},
            error_type=type(exc).__name__,
        )
        print(json.dumps(report.to_dict(), sort_keys=True), file=sys.stderr)
        if args.debug:
            logger.exception("Kafka diagnostic configuration failed")
        return 3
    print(json.dumps(report.to_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
