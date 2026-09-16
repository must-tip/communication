from __future__ import annotations

import os
import re
from pathlib import Path
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from .errors import ConfigurationError

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_TOPIC_PART_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,248}$", re.ASCII)
_ALLOWED_MODES = frozenset({"primary", "failover", "all", "any"})
_ALLOWED_TRANSPORTS = frozenset({"kafka", "nats"})
_KAFKA_SETTING_KEYS = frozenset(
    {
        "ACKS",
        "BOOTSTRAP_SERVERS",
        "CLIENT_ID",
        "CONSUMER",
        "CONSUMER_CALLBACK_TIMEOUT_SECONDS",
        "CONSUMER_MAX_POLL_INTERVAL_MS",
        "DEFAULT_TOPIC",
        "DLQ_SUFFIX",
        "EXTRA_PRODUCER_CONFIG",
        "PASSWORD",
        "PRODUCER",
        "RETRIES",
        "RETRY_BACKOFF_SECONDS",
        "SASL_MECHANISM",
        "SCHEMA_VERSION",
        "SECURITY_PROTOCOL",
        "SSL_CA_LOCATION",
        "TOPIC_PREFIX",
        "TRANSACTIONAL_ID",
        "USERNAME",
    }
)
_KAFKA_DOTTED_ALIASES = MappingProxyType(
    {
        "acks": "ACKS",
        "bootstrap.servers": "BOOTSTRAP_SERVERS",
        "client.id": "CLIENT_ID",
        "sasl.mechanism": "SASL_MECHANISM",
        "sasl.password": "PASSWORD",
        "sasl.username": "USERNAME",
        "security.protocol": "SECURITY_PROTOCOL",
        "ssl.ca.location": "SSL_CA_LOCATION",
        "transactional.id": "TRANSACTIONAL_ID",
    }
)
_KAFKA_PRODUCER_ONLY_OPTIONS = frozenset(
    {
        "batch.num.messages",
        "batch.size",
        "compression.type",
        "delivery.report.only.error",
        "delivery.timeout.ms",
        "enable.gapless.guarantee",
        "enable.idempotence",
        "linger.ms",
        "max.in.flight.requests.per.connection",
        "message.timeout.ms",
        "partitioner",
        "queue.buffering.max.kbytes",
        "queue.buffering.max.messages",
        "queue.buffering.max.ms",
        "sticky.partitioning.linger.ms",
        "transaction.timeout.ms",
    }
)




def _read_text_file(path_value: object, *, name: str, maximum_bytes: int = 65_536) -> str:
    path_text = _safe_text(path_value, "", name, 4096)
    path = Path(path_text)
    try:
        stat = path.stat()
    except OSError as exc:
        raise ConfigurationError(f"{name} cannot be read") from exc
    if not path.is_file():
        raise ConfigurationError(f"{name} must reference a regular file")
    if stat.st_size > maximum_bytes:
        raise ConfigurationError(f"{name} is too large")
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as exc:
        raise ConfigurationError(f"{name} cannot be read as UTF-8") from exc
    if not value or _CONTROL_RE.search(value):
        raise ConfigurationError(f"{name} contains an invalid value")
    return value


def _first_nonempty(mapping: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = mapping.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _value_or_file(
    mapping: Mapping[str, str],
    *,
    value_names: Sequence[str],
    file_names: Sequence[str] = (),
    required: bool = False,
    label: str,
) -> str | None:
    value = _first_nonempty(mapping, *value_names)
    if value is not None:
        return value
    file_value = _first_nonempty(mapping, *file_names)
    if file_value is not None:
        return _read_text_file(file_value, name=f"{label}_FILE")
    if required:
        accepted = ", ".join((*value_names, *file_names))
        raise ConfigurationError(f"{label} is required; expected one of: {accepted}")
    return None


def _strict_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ConfigurationError("boolean configuration contains an invalid value")


def _bounded_int(value: object, default: int, minimum: int, maximum: int, name: str) -> int:
    if value is None:
        value = default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if not minimum <= parsed <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return parsed


def _bounded_float(value: object, default: float, minimum: float, maximum: float, name: str) -> float:
    if value is None:
        value = default
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be numeric") from exc
    if not minimum <= parsed <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return parsed


def _safe_text(value: object, default: str, name: str, maximum_length: int = 255) -> str:
    candidate = default if value is None else str(value).strip()
    if not candidate or len(candidate) > maximum_length or _CONTROL_RE.search(candidate):
        raise ConfigurationError(f"{name} is invalid")
    return candidate


def _sequence(value: object, default: Sequence[str], name: str) -> tuple[str, ...]:
    if value is None:
        raw = list(default)
    elif isinstance(value, str):
        raw = [item.strip() for item in value.split(",")]
    elif isinstance(value, (list, tuple, set, frozenset)):
        raw = [str(item).strip() for item in value]
    else:
        raise ConfigurationError(f"{name} must be a sequence")
    output: list[str] = []
    seen: set[str] = set()
    for item in raw:
        if not item:
            continue
        normalized = item.casefold()
        if normalized not in seen:
            seen.add(normalized)
            output.append(item)
    return tuple(output)


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{name} must be a mapping")
    return MappingProxyType(dict(value))


def _normalized_kafka_mapping(
    value: object,
    *,
    name: str,
    extras_target: str = "PRODUCER",
) -> dict[str, Any]:
    """Accept both communication-style and librdkafka-style Kafka settings.

    The Kafka platform's Django contract uses dotted librdkafka keys. Existing
    communication deployments use upper-case nested keys. Normalizing both here
    prevents a valid platform configuration from silently falling back to the
    local PLAINTEXT defaults.
    """

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{name} must be a mapping")
    if extras_target not in {"PRODUCER", "CONSUMER"}:
        raise ValueError("extras_target must be PRODUCER or CONSUMER")

    normalized: dict[str, Any] = {}
    extras: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key).strip()
        upper_key = key.upper()
        if upper_key in _KAFKA_SETTING_KEYS:
            normalized[upper_key] = raw_value
            continue
        alias = _KAFKA_DOTTED_ALIASES.get(key.casefold())
        if alias is not None:
            normalized.setdefault(alias, raw_value)
            continue
        if key.casefold() == "retry.backoff.ms":
            try:
                retry_backoff_ms = float(raw_value)
            except (TypeError, ValueError) as exc:
                raise ConfigurationError(f"{name}.retry.backoff.ms must be numeric") from exc
            normalized.setdefault("RETRY_BACKOFF_SECONDS", retry_backoff_ms / 1000.0)
            continue
        if key.casefold() == "retries":
            normalized.setdefault("RETRIES", raw_value)
            continue
        if (
            extras_target == "CONSUMER"
            and key.casefold() in _KAFKA_PRODUCER_ONLY_OPTIONS
        ):
            continue
        extras[key] = raw_value

    existing_extras = normalized.get(extras_target)
    if existing_extras is not None and not isinstance(existing_extras, Mapping):
        raise ConfigurationError(f"{name}.{extras_target} must be a mapping")
    if extras or existing_extras:
        normalized[extras_target] = {**extras, **dict(existing_extras or {})}
    return normalized


def _merge_kafka_mappings(
    inherited: Mapping[str, Any],
    explicit: Mapping[str, Any],
) -> dict[str, Any]:
    """Merge normalized Kafka mappings while preserving nested client options."""

    output = {**inherited, **explicit}
    for section in ("PRODUCER", "CONSUMER", "EXTRA_PRODUCER_CONFIG"):
        inherited_section = inherited.get(section)
        explicit_section = explicit.get(section)
        if inherited_section is not None or explicit_section is not None:
            if inherited_section is not None and not isinstance(inherited_section, Mapping):
                raise ConfigurationError(f"KAFKA.{section} must be a mapping")
            if explicit_section is not None and not isinstance(explicit_section, Mapping):
                raise ConfigurationError(f"KAFKA.{section} must be a mapping")
            output[section] = {
                **dict(inherited_section or {}),
                **dict(explicit_section or {}),
            }
    return output


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: tuple[str, ...] = ("localhost:9092",)
    client_id: str = "must-tip"
    producer: Mapping[str, Any] = field(default_factory=dict, repr=False)
    consumer: Mapping[str, Any] = field(default_factory=dict, repr=False)
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    username: str | None = field(default=None, repr=False)
    password: str | None = field(default=None, repr=False)
    ssl_ca_location: str | None = None
    transactional_id: str | None = None
    topic_prefix: str = "svc"
    default_topic: str = "events"
    schema_version: str = "v1"
    dlq_suffix: str = "dlq"
    retries: int = 5
    retry_backoff_seconds: float = 0.5
    acks: str = "all"
    consumer_callback_timeout_seconds: float = 600.0
    consumer_max_poll_interval_ms: int = 900_000
    extra_producer_config: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        servers = (
            tuple(item.strip() for item in self.bootstrap_servers.split(",") if item.strip())
            if isinstance(self.bootstrap_servers, str)
            else tuple(self.bootstrap_servers)
        )
        if not servers or len(servers) > 64:
            raise ConfigurationError("Kafka bootstrap_servers must not be empty")
        for server in servers:
            if (
                not server
                or len(server) > 512
                or _CONTROL_RE.search(server)
                or "://" in server
                or "/" in server
                or "@" in server
                or any(character.isspace() for character in server)
            ):
                raise ConfigurationError("Kafka bootstrap_servers contains an invalid server")
            host, separator, port = server.rpartition(":")
            if not separator or not host or not port.isdigit() or not 1 <= int(port) <= 65_535:
                raise ConfigurationError("Kafka bootstrap_servers entries must use host:port")
        protocol = self.security_protocol.upper()
        if protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ConfigurationError("Kafka security_protocol is invalid")
        if protocol.startswith("SASL") and (not self.sasl_mechanism or not self.username or not self.password):
            raise ConfigurationError("Kafka SASL requires mechanism, username, and password")
        if protocol == "SASL_SSL" and not self.ssl_ca_location:
            raise ConfigurationError("Kafka SASL_SSL requires ssl_ca_location")
        if self.acks not in {"all", "-1", "1", "0"}:
            raise ConfigurationError("Kafka acks must be one of all, -1, 1, or 0")
        if self.transactional_id:
            raise ConfigurationError(
                "Kafka transactional_id is not supported by the generic EventBus; "
                "use the durable outbox or a dedicated transaction-aware workflow"
            )
        if self.retries < 0 or self.retry_backoff_seconds < 0:
            raise ConfigurationError("Kafka retry settings must be non-negative")
        if not 1.0 <= float(self.consumer_callback_timeout_seconds) <= 86_400.0:
            raise ConfigurationError("Kafka consumer_callback_timeout_seconds must be between 1 and 86400")
        if not 30_000 <= int(self.consumer_max_poll_interval_ms) <= 86_400_000:
            raise ConfigurationError("Kafka consumer_max_poll_interval_ms must be between 30000 and 86400000")
        callback_ms = int(float(self.consumer_callback_timeout_seconds) * 1000)
        if callback_ms + 30_000 >= int(self.consumer_max_poll_interval_ms):
            raise ConfigurationError(
                "Kafka consumer callback timeout must leave at least 30 seconds before max.poll.interval.ms"
            )
        _safe_text(self.client_id, "must-tip", "Kafka client_id")
        for name in ("topic_prefix", "default_topic", "schema_version", "dlq_suffix"):
            _safe_text(getattr(self, name), "", f"Kafka {name}")
        object.__setattr__(self, "bootstrap_servers", ",".join(servers))
        object.__setattr__(self, "security_protocol", protocol)
        object.__setattr__(self, "producer", MappingProxyType(dict(self.producer)))
        object.__setattr__(self, "consumer", MappingProxyType(dict(self.consumer)))
        object.__setattr__(self, "extra_producer_config", MappingProxyType(dict(self.extra_producer_config)))

    def producer_config(self) -> dict[str, Any]:
        config: dict[str, Any] = {
            "compression.type": "zstd",
            "delivery.timeout.ms": 120_000,
            "request.timeout.ms": 30_000,
        }
        config.update(dict(self.extra_producer_config))
        config.update(dict(self.producer))
        # Dedicated fields are authoritative. Client option dictionaries may
        # tune batching and timeouts but cannot disable the platform's delivery,
        # identity, authentication, or topic-provisioning safety controls.
        config.update(
            {
                "bootstrap.servers": str(self.bootstrap_servers),
                "client.id": self.client_id,
                "security.protocol": self.security_protocol,
                "enable.idempotence": self.acks in {"all", "-1"},
                "acks": self.acks,
                "retries": (
                    2_147_483_647 if self.acks in {"all", "-1"} else self.retries
                ),
                "retry.backoff.ms": int(self.retry_backoff_seconds * 1000),
                "max.in.flight.requests.per.connection": 5,
                "allow.auto.create.topics": False,
                "enable.ssl.certificate.verification": True,
                "ssl.endpoint.identification.algorithm": "https",
            }
        )
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.username
            config["sasl.password"] = self.password
        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config

    def consumer_config(self, group_id: str) -> dict[str, Any]:
        normalized_group_id = _safe_text(group_id, "", "Kafka group_id")
        config: dict[str, Any] = {
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45_000,
            "max.poll.interval.ms": int(self.consumer_max_poll_interval_ms),
        }
        config.update(dict(self.consumer))
        config.update(
            {
                "bootstrap.servers": str(self.bootstrap_servers),
                "client.id": self.client_id,
                "group.id": normalized_group_id,
                "security.protocol": self.security_protocol,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "isolation.level": "read_committed",
                "allow.auto.create.topics": False,
                "enable.ssl.certificate.verification": True,
                "ssl.endpoint.identification.algorithm": "https",
            }
        )
        if self.sasl_mechanism:
            config["sasl.mechanism"] = self.sasl_mechanism
            config["sasl.username"] = self.username
            config["sasl.password"] = self.password
        if self.ssl_ca_location:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config

    def _topic_base(self) -> str:
        return f"{self.topic_prefix}.{self.schema_version}"

    def topic_components(self, topic: str) -> tuple[str, ...]:
        if not isinstance(topic, str):
            raise ConfigurationError("topic must be a string")
        return tuple(item for item in re.sub(r"\s+", ".", topic.strip().strip(".").casefold()).split(".") if item)

    def is_fully_qualified(self, topic: str) -> bool:
        parts = self.topic_components(topic)
        return bool(parts) and (parts[0] == self.topic_prefix.casefold() or any(re.fullmatch(r"v\d+", part) for part in parts) or len(parts) >= 3)

    def full_topic_name(self, topic: str | None = None) -> str:
        selected = topic or self.default_topic
        parts = self.topic_components(selected)
        if not parts:
            parts = self.topic_components(self.default_topic)
        if self.is_fully_qualified(".".join(parts)):
            return ".".join(parts)
        return ".".join((*self.topic_components(self._topic_base()), *parts))

    def normalize_topic(self, topic: str) -> str:
        return self.full_topic_name(topic)

    def dlq_topic_name(self, topic: str | None = None) -> str:
        return f"{self.full_topic_name(topic)}.{self.dlq_suffix}"

    def as_dict(self) -> dict[str, object]:
        return dict(self.redacted())

    @classmethod
    def from_django_settings(cls, settings_module: object) -> "KafkaSettings":
        return CommunicationSettings.from_django_settings(settings_module).kafka

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        service_name: str | None = None,
    ) -> "KafkaSettings":
        return CommunicationSettings.from_environment(
            environ, service_name=service_name
        ).kafka

    def redacted(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "bootstrap_servers": self.bootstrap_servers,
                "client_id": self.client_id,
                "security_protocol": self.security_protocol,
                "sasl_mechanism": self.sasl_mechanism,
                "ssl_ca_location": self.ssl_ca_location,
                "transactional_id": self.transactional_id,
                "topic_prefix": self.topic_prefix,
                "default_topic": self.default_topic,
                "schema_version": self.schema_version,
                "dlq_suffix": self.dlq_suffix,
                "retries": self.retries,
                "retry_backoff_seconds": self.retry_backoff_seconds,
                "acks": self.acks,
                "consumer_callback_timeout_seconds": self.consumer_callback_timeout_seconds,
                "consumer_max_poll_interval_ms": self.consumer_max_poll_interval_ms,
                "producer_keys": tuple(sorted(set(self.producer) | set(self.extra_producer_config))),
                "consumer_keys": tuple(sorted(self.consumer)),
            }
        )


@dataclass(frozen=True, slots=True)
class NatsSettings:
    servers: tuple[str, ...] = ("nats://localhost:4222",)
    name: str = "must-tip"
    token: str | None = field(default=None, repr=False)
    user_credentials: str | None = None
    user: str | None = None
    password: str | None = field(default=None, repr=False)
    tls_ca_file: str | None = None
    tls_cert_file: str | None = None
    tls_key_file: str | None = field(default=None, repr=False)
    tls_hostname: str | None = None
    require_tls: bool = False
    jetstream: bool = True
    stream: str | None = None
    publish_timeout_seconds: float = 5.0
    ack_wait_seconds: int = 60
    max_deliver: int = 5
    extra: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.servers or len(self.servers) > 64:
            raise ConfigurationError("NATS servers must not be empty")
        tls_servers = 0
        for server in self.servers:
            if not server or len(server) > 512 or _CONTROL_RE.search(server):
                raise ConfigurationError("NATS servers contains an invalid server")
            if server.startswith("tls://"):
                tls_servers += 1
            elif not server.startswith("nats://"):
                raise ConfigurationError("NATS server URLs must use nats:// or tls://")
        if self.require_tls and tls_servers != len(self.servers):
            raise ConfigurationError(
                "NATS require_tls forbids plaintext nats:// server URLs"
            )
        if 0 < tls_servers < len(self.servers):
            raise ConfigurationError(
                "NATS TLS and plaintext server URLs must not be mixed"
            )
        if self.token and (self.user_credentials or self.user or self.password):
            raise ConfigurationError("NATS authentication methods must not be combined")
        if self.user_credentials and (self.user or self.password):
            raise ConfigurationError("NATS credentials and user/password must not be combined")
        if bool(self.user) != bool(self.password):
            raise ConfigurationError("NATS user and password must be configured together")
        if bool(self.tls_cert_file) != bool(self.tls_key_file):
            raise ConfigurationError("NATS TLS certificate and key must be configured together")
        object.__setattr__(self, "extra", MappingProxyType(dict(self.extra)))

    def redacted(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "servers": self.servers,
                "name": self.name,
                "authentication": "token" if self.token else "creds" if self.user_credentials else "user_password" if self.user else "none",
                "tls_ca_file": self.tls_ca_file,
                "tls_cert_file": self.tls_cert_file,
                "tls_hostname": self.tls_hostname,
                "require_tls": self.require_tls,
                "jetstream": self.jetstream,
                "stream": self.stream,
                "publish_timeout_seconds": self.publish_timeout_seconds,
                "ack_wait_seconds": self.ack_wait_seconds,
                "max_deliver": self.max_deliver,
                "extra_keys": tuple(sorted(self.extra)),
            }
        )


@dataclass(frozen=True, slots=True)
class CommunicationSettings:
    service_name: str = "authentication"
    enabled_transports: tuple[str, ...] = ("nats",)
    primary_transport: str = "nats"
    publish_mode: str = "primary"
    consumer_transports: tuple[str, ...] = ("nats",)
    default_topic: str = "accounts.events"
    topic_prefix: str = ""
    schema_version: str = "v1"
    publish_timeout_seconds: float = 5.0
    flush_timeout_seconds: float = 10.0
    max_event_bytes: int = 1_048_576
    max_headers: int = 64
    max_header_value_length: int = 2048
    handler_retries: int = 3
    retry_backoff_seconds: float = 0.25
    retry_max_seconds: float = 30.0
    dlq_suffix: str = ".dlq"
    consumer_concurrency: int = 4
    consumer_max_pending: int = 2_000
    graceful_shutdown_seconds: float = 30.0
    require_durable_consumers: bool = False
    idempotency_ttl_seconds: int = 86_400
    idempotency_fail_closed: bool = True
    signing_required: bool = False
    signing_key_id: str | None = None
    signing_keys: Mapping[str, str] = field(default_factory=dict, repr=False)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    nats: NatsSettings = field(default_factory=NatsSettings)

    def __post_init__(self) -> None:
        enabled = tuple(dict.fromkeys(item.casefold() for item in self.enabled_transports))
        consumers = tuple(dict.fromkeys(item.casefold() for item in self.consumer_transports))
        unknown = (set(enabled) | set(consumers)) - _ALLOWED_TRANSPORTS
        if unknown:
            raise ConfigurationError(f"unknown communication transports: {sorted(unknown)}")
        primary = self.primary_transport.casefold()
        if enabled and primary not in enabled:
            raise ConfigurationError("primary_transport must be enabled")
        mode = self.publish_mode.casefold()
        if mode not in _ALLOWED_MODES:
            raise ConfigurationError(f"publish_mode must be one of {sorted(_ALLOWED_MODES)}")
        if mode == "all" and len(enabled) < 2:
            raise ConfigurationError("publish_mode='all' requires at least two transports")
        if self.require_durable_consumers and "nats" in consumers and not self.nats.jetstream:
            raise ConfigurationError("durable NATS consumption requires JetStream")
        if self.signing_required and not self.signing_keys:
            raise ConfigurationError("signing_required requires signing_keys")
        if self.signing_key_id and self.signing_key_id not in self.signing_keys:
            raise ConfigurationError("signing_key_id is not present in signing_keys")
        for key_id, secret in self.signing_keys.items():
            if not isinstance(key_id, str) or not key_id or len(key_id) > 64:
                raise ConfigurationError("event signing key id is invalid")
            if not isinstance(secret, str) or len(secret) < 32:
                raise ConfigurationError("event signing secrets must contain at least 32 characters")
        object.__setattr__(self, "enabled_transports", enabled)
        object.__setattr__(self, "consumer_transports", consumers)
        object.__setattr__(self, "primary_transport", primary)
        object.__setattr__(self, "publish_mode", mode)
        object.__setattr__(self, "service_name", _safe_text(self.service_name, "authentication", "service_name"))
        object.__setattr__(self, "default_topic", self.canonical_topic(self.default_topic))
        object.__setattr__(self, "schema_version", _safe_text(self.schema_version, "v1", "schema_version", 32))
        object.__setattr__(self, "signing_keys", MappingProxyType(dict(self.signing_keys)))

    def canonical_topic(self, topic: str | None) -> str:
        candidate = str(topic or self.default_topic).strip().strip(".")
        if self.topic_prefix:
            prefix = self.topic_prefix.strip().strip(".")
            if not candidate.startswith(prefix + "."):
                candidate = f"{prefix}.{candidate}"
        candidate = re.sub(r"\.+", ".", candidate)
        if len(candidate) > 249 or _TOPIC_PART_RE.fullmatch(candidate) is None:
            raise ConfigurationError("event topic is invalid")
        return candidate

    def redacted(self) -> Mapping[str, object]:
        return MappingProxyType(
            {
                "service_name": self.service_name,
                "enabled_transports": self.enabled_transports,
                "primary_transport": self.primary_transport,
                "publish_mode": self.publish_mode,
                "consumer_transports": self.consumer_transports,
                "default_topic": self.default_topic,
                "schema_version": self.schema_version,
                "publish_timeout_seconds": self.publish_timeout_seconds,
                "max_event_bytes": self.max_event_bytes,
                "handler_retries": self.handler_retries,
                "consumer_concurrency": self.consumer_concurrency,
                "require_durable_consumers": self.require_durable_consumers,
                "signing_required": self.signing_required,
                "signing_key_ids": tuple(sorted(self.signing_keys)),
                "kafka": self.kafka.redacted(),
                "nats": self.nats.redacted(),
            }
        )

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> "CommunicationSettings":
        cfg = dict(raw or {})
        kafka_raw = _normalized_kafka_mapping(
            cfg.get("KAFKA"),
            name="KAFKA",
            extras_target="PRODUCER",
        )
        nats_raw = dict(cfg.get("NATS") or {})
        kafka = KafkaSettings(
            bootstrap_servers=tuple(_sequence(kafka_raw.get("BOOTSTRAP_SERVERS"), ("localhost:9092",), "KAFKA.BOOTSTRAP_SERVERS")),
            client_id=_safe_text(kafka_raw.get("CLIENT_ID"), str(cfg.get("SERVICE_NAME") or "must-tip"), "KAFKA.CLIENT_ID"),
            producer=_mapping(kafka_raw.get("PRODUCER"), "KAFKA.PRODUCER"),
            consumer=_mapping(kafka_raw.get("CONSUMER"), "KAFKA.CONSUMER"),
            security_protocol=str(kafka_raw.get("SECURITY_PROTOCOL") or "PLAINTEXT"),
            sasl_mechanism=kafka_raw.get("SASL_MECHANISM"),
            username=kafka_raw.get("USERNAME"),
            password=kafka_raw.get("PASSWORD"),
            ssl_ca_location=kafka_raw.get("SSL_CA_LOCATION"),
            transactional_id=kafka_raw.get("TRANSACTIONAL_ID"),
            topic_prefix=str(kafka_raw.get("TOPIC_PREFIX") or "svc"),
            default_topic=str(kafka_raw.get("DEFAULT_TOPIC") or "events"),
            schema_version=str(kafka_raw.get("SCHEMA_VERSION") or "v1"),
            dlq_suffix=str(kafka_raw.get("DLQ_SUFFIX") or "dlq"),
            retries=_bounded_int(kafka_raw.get("RETRIES"), 5, 0, 2_147_483_647, "KAFKA.RETRIES"),
            retry_backoff_seconds=_bounded_float(kafka_raw.get("RETRY_BACKOFF_SECONDS"), 0.5, 0.0, 60.0, "KAFKA.RETRY_BACKOFF_SECONDS"),
            acks=str(kafka_raw.get("ACKS") or "all"),
            consumer_callback_timeout_seconds=_bounded_float(kafka_raw.get("CONSUMER_CALLBACK_TIMEOUT_SECONDS"), 600.0, 1.0, 86_400.0, "KAFKA.CONSUMER_CALLBACK_TIMEOUT_SECONDS"),
            consumer_max_poll_interval_ms=_bounded_int(kafka_raw.get("CONSUMER_MAX_POLL_INTERVAL_MS"), 900_000, 30_000, 86_400_000, "KAFKA.CONSUMER_MAX_POLL_INTERVAL_MS"),
            extra_producer_config=_mapping(kafka_raw.get("EXTRA_PRODUCER_CONFIG"), "KAFKA.EXTRA_PRODUCER_CONFIG"),
        )
        nats = NatsSettings(
            servers=tuple(_sequence(nats_raw.get("SERVERS"), ("nats://localhost:4222",), "NATS.SERVERS")),
            name=_safe_text(nats_raw.get("NAME"), str(cfg.get("SERVICE_NAME") or "must-tip"), "NATS.NAME"),
            token=nats_raw.get("TOKEN"),
            user_credentials=nats_raw.get("CREDENTIALS_FILE"),
            user=nats_raw.get("USER"),
            password=nats_raw.get("PASSWORD"),
            tls_ca_file=nats_raw.get("TLS_CA_FILE"),
            tls_cert_file=nats_raw.get("TLS_CERT_FILE"),
            tls_key_file=nats_raw.get("TLS_KEY_FILE"),
            tls_hostname=nats_raw.get("TLS_HOSTNAME"),
            require_tls=_strict_bool(nats_raw.get("REQUIRE_TLS"), False),
            jetstream=_strict_bool(nats_raw.get("JETSTREAM"), True),
            stream=nats_raw.get("STREAM"),
            publish_timeout_seconds=_bounded_float(nats_raw.get("PUBLISH_TIMEOUT_SECONDS"), 5.0, 0.1, 120.0, "NATS.PUBLISH_TIMEOUT_SECONDS"),
            ack_wait_seconds=_bounded_int(nats_raw.get("ACK_WAIT_SECONDS"), 60, 1, 86_400, "NATS.ACK_WAIT_SECONDS"),
            max_deliver=_bounded_int(nats_raw.get("MAX_DELIVER"), 5, 1, 1000, "NATS.MAX_DELIVER"),
            extra=_mapping(nats_raw.get("EXTRA"), "NATS.EXTRA"),
        )
        signing_keys = cfg.get("SIGNING_KEYS") or {}
        return cls(
            service_name=_safe_text(cfg.get("SERVICE_NAME"), "authentication", "SERVICE_NAME"),
            enabled_transports=_sequence(cfg.get("ENABLED_TRANSPORTS"), ("nats",), "ENABLED_TRANSPORTS"),
            primary_transport=_safe_text(cfg.get("PRIMARY_TRANSPORT"), "nats", "PRIMARY_TRANSPORT", 32),
            publish_mode=_safe_text(cfg.get("PUBLISH_MODE"), "primary", "PUBLISH_MODE", 32),
            consumer_transports=_sequence(cfg.get("CONSUMER_TRANSPORTS"), _sequence(cfg.get("ENABLED_TRANSPORTS"), ("nats",), "ENABLED_TRANSPORTS"), "CONSUMER_TRANSPORTS"),
            default_topic=_safe_text(cfg.get("DEFAULT_TOPIC"), "accounts.events", "DEFAULT_TOPIC"),
            topic_prefix=str(cfg.get("TOPIC_PREFIX") or "").strip(),
            schema_version=_safe_text(cfg.get("SCHEMA_VERSION"), "v1", "SCHEMA_VERSION", 32),
            publish_timeout_seconds=_bounded_float(cfg.get("PUBLISH_TIMEOUT_SECONDS"), 5.0, 0.1, 300.0, "PUBLISH_TIMEOUT_SECONDS"),
            flush_timeout_seconds=_bounded_float(cfg.get("FLUSH_TIMEOUT_SECONDS"), 10.0, 0.1, 300.0, "FLUSH_TIMEOUT_SECONDS"),
            max_event_bytes=_bounded_int(cfg.get("MAX_EVENT_BYTES"), 1_048_576, 1024, 16_777_216, "MAX_EVENT_BYTES"),
            max_headers=_bounded_int(cfg.get("MAX_HEADERS"), 64, 0, 256, "MAX_HEADERS"),
            max_header_value_length=_bounded_int(cfg.get("MAX_HEADER_VALUE_LENGTH"), 2048, 16, 16_384, "MAX_HEADER_VALUE_LENGTH"),
            handler_retries=_bounded_int(cfg.get("HANDLER_RETRIES"), 3, 0, 20, "HANDLER_RETRIES"),
            retry_backoff_seconds=_bounded_float(cfg.get("RETRY_BACKOFF_SECONDS"), 0.25, 0.0, 60.0, "RETRY_BACKOFF_SECONDS"),
            retry_max_seconds=_bounded_float(cfg.get("RETRY_MAX_SECONDS"), 30.0, 0.1, 3600.0, "RETRY_MAX_SECONDS"),
            dlq_suffix=_safe_text(cfg.get("DLQ_SUFFIX"), ".dlq", "DLQ_SUFFIX", 32),
            consumer_concurrency=_bounded_int(cfg.get("CONSUMER_CONCURRENCY"), 4, 1, 128, "CONSUMER_CONCURRENCY"),
            consumer_max_pending=_bounded_int(cfg.get("CONSUMER_MAX_PENDING"), 2000, 1, 1_000_000, "CONSUMER_MAX_PENDING"),
            graceful_shutdown_seconds=_bounded_float(cfg.get("GRACEFUL_SHUTDOWN_SECONDS"), 30.0, 1.0, 600.0, "GRACEFUL_SHUTDOWN_SECONDS"),
            require_durable_consumers=_strict_bool(cfg.get("REQUIRE_DURABLE_CONSUMERS"), False),
            idempotency_ttl_seconds=_bounded_int(cfg.get("IDEMPOTENCY_TTL_SECONDS"), 86_400, 60, 31_536_000, "IDEMPOTENCY_TTL_SECONDS"),
            idempotency_fail_closed=_strict_bool(cfg.get("IDEMPOTENCY_FAIL_CLOSED"), True),
            signing_required=_strict_bool(cfg.get("SIGNING_REQUIRED"), False),
            signing_key_id=cfg.get("SIGNING_KEY_ID"),
            signing_keys=_mapping(signing_keys, "SIGNING_KEYS"),
            kafka=kafka,
            nats=nats,
        )

    @classmethod
    def from_environment(
        cls,
        environ: Mapping[str, str] | None = None,
        *,
        service_name: str | None = None,
    ) -> "CommunicationSettings":
        """Build settings directly from the Kubernetes workload contract.

        The cloud-services control plane injects Kafka values through environment
        variables and mounted Secret files. This method accepts both the current
        platform names and legacy aliases so SDK diagnostics can run without a
        Django settings module. Secret values are read without being exposed in
        repr/redacted output.
        """
        env = dict(os.environ if environ is None else environ)
        enabled = _first_nonempty(env, "COMMUNICATION_ENABLED_TRANSPORTS") or "kafka"
        consumers = _first_nonempty(env, "COMMUNICATION_CONSUMER_TRANSPORTS") or enabled
        primary = _first_nonempty(env, "COMMUNICATION_PRIMARY_TRANSPORT") or _sequence(enabled, ("kafka",), "COMMUNICATION_ENABLED_TRANSPORTS")[0]
        enabled_set = {item.casefold() for item in _sequence(enabled, ("kafka",), "COMMUNICATION_ENABLED_TRANSPORTS")}
        kafka_required = "kafka" in enabled_set or "kafka" in {item.casefold() for item in _sequence(consumers, (), "COMMUNICATION_CONSUMER_TRANSPORTS")}

        username = _value_or_file(
            env,
            value_names=("KAFKA_USERNAME", "KAFKA_SASL_USERNAME"),
            file_names=("KAFKA_USERNAME_FILE",),
            required=kafka_required,
            label="KAFKA_USERNAME",
        )
        password = _value_or_file(
            env,
            value_names=("KAFKA_PASSWORD", "KAFKA_SASL_PASSWORD"),
            file_names=("KAFKA_PASSWORD_FILE",),
            required=kafka_required,
            label="KAFKA_PASSWORD",
        )
        ca_file = _first_nonempty(env, "KAFKA_CA_FILE", "KAFKA_SSL_CA_LOCATION")
        bootstrap = _first_nonempty(env, "KAFKA_BOOTSTRAP_SERVERS")
        if kafka_required and not bootstrap:
            raise ConfigurationError("KAFKA_BOOTSTRAP_SERVERS is required when Kafka is enabled")

        resolved_service = service_name or _first_nonempty(env, "SERVICE_NAME", "APP_NAME", "OTEL_SERVICE_NAME") or "must-tip"
        kafka_mapping: dict[str, object] = {
            "BOOTSTRAP_SERVERS": bootstrap or "localhost:9092",
            "CLIENT_ID": _first_nonempty(env, "KAFKA_CLIENT_ID") or resolved_service,
            "SECURITY_PROTOCOL": _first_nonempty(env, "KAFKA_SECURITY_PROTOCOL") or ("SASL_SSL" if kafka_required else "PLAINTEXT"),
            "SASL_MECHANISM": _first_nonempty(env, "KAFKA_SASL_MECHANISM") or ("SCRAM-SHA-512" if kafka_required else None),
            "USERNAME": username,
            "PASSWORD": password,
            "SSL_CA_LOCATION": ca_file,
            "TOPIC_PREFIX": _first_nonempty(env, "KAFKA_TOPIC_PREFIX", "COMMUNICATION_TOPIC_PREFIX") or "svc",
            "DEFAULT_TOPIC": _first_nonempty(env, "KAFKA_DEFAULT_TOPIC", "COMMUNICATION_DEFAULT_TOPIC") or "events",
            "ACKS": _first_nonempty(env, "KAFKA_ACKS") or "all",
        }
        if kafka_required and str(kafka_mapping["SECURITY_PROTOCOL"]).upper() == "SASL_SSL":
            if not ca_file:
                raise ConfigurationError("KAFKA_CA_FILE or KAFKA_SSL_CA_LOCATION is required for SASL_SSL")
            if not Path(ca_file).is_file():
                raise ConfigurationError("Kafka CA file does not exist or is not a regular file")

        return cls.from_mapping(
            {
                "SERVICE_NAME": resolved_service,
                "ENABLED_TRANSPORTS": enabled,
                "PRIMARY_TRANSPORT": primary,
                "CONSUMER_TRANSPORTS": consumers,
                "PUBLISH_MODE": _first_nonempty(env, "COMMUNICATION_PUBLISH_MODE") or "primary",
                "DEFAULT_TOPIC": _first_nonempty(env, "COMMUNICATION_DEFAULT_TOPIC", "KAFKA_DEFAULT_TOPIC") or "events",
                "TOPIC_PREFIX": _first_nonempty(env, "COMMUNICATION_TOPIC_PREFIX", "KAFKA_TOPIC_PREFIX") or "",
                "KAFKA": kafka_mapping,
            }
        )

    @classmethod
    def from_django_settings(cls, settings_module: object | None = None) -> "CommunicationSettings":
        if settings_module is None:
            try:
                from django.conf import settings as django_settings
            except ImportError as exc:
                raise ConfigurationError("Django is not installed; pass an explicit settings mapping") from exc
            settings_module = django_settings
        raw = getattr(settings_module, "COMMUNICATION", None)
        if raw is None and isinstance(settings_module, Mapping):
            raw = settings_module.get("COMMUNICATION")
        if raw is not None:
            if not isinstance(raw, Mapping):
                raise ConfigurationError("COMMUNICATION must be a mapping")
            merged = dict(raw)
        else:
            merged = {}

        def inherited(name: str, default: object = None) -> object:
            if isinstance(settings_module, Mapping):
                return settings_module.get(name, default)
            return getattr(settings_module, name, default)

        # Backward-compatible legacy settings. Explicit COMMUNICATION values win.
        merged.setdefault("SERVICE_NAME", inherited("SERVICE_NAME", "authentication"))
        merged.setdefault("DEFAULT_TOPIC", inherited("ACCOUNTS_DEFAULT_TOPIC", inherited("KAFKA_DEFAULT_TOPIC", "accounts.events")))
        if "ENABLED_TRANSPORTS" not in merged:
            backend = str(inherited("COMMUNICATION_BACKEND", "nats")).casefold()
            merged["ENABLED_TRANSPORTS"] = ("kafka", "nats") if backend in {"both", "all"} else (backend,)
        merged["ENABLED_TRANSPORTS"] = _sequence(
            merged["ENABLED_TRANSPORTS"], ("nats",), "ENABLED_TRANSPORTS"
        )
        default_primary = (
            merged["ENABLED_TRANSPORTS"][0]
            if merged["ENABLED_TRANSPORTS"]
            else "nats"
        )
        merged.setdefault(
            "PRIMARY_TRANSPORT",
            inherited("COMMUNICATION_PRIMARY_TRANSPORT", default_primary),
        )
        merged.setdefault("PUBLISH_MODE", inherited("COMMUNICATION_PUBLISH_MODE", "primary"))
        merged.setdefault("CONSUMER_TRANSPORTS", inherited("COMMUNICATION_CONSUMER_TRANSPORTS", merged["ENABLED_TRANSPORTS"]))
        merged.setdefault("MAX_EVENT_BYTES", inherited("MAX_EVENT_BYTES", 1_048_576))

        kafka = _normalized_kafka_mapping(
            merged.get("KAFKA"),
            name="COMMUNICATION.KAFKA",
            extras_target="PRODUCER",
        )
        legacy_kafka = _normalized_kafka_mapping(
            inherited("KAFKA", {}) or {},
            name="KAFKA",
            extras_target="PRODUCER",
        )
        legacy_consumer = _normalized_kafka_mapping(
            inherited("KAFKA_CONSUMER", {}) or {},
            name="KAFKA_CONSUMER",
            extras_target="CONSUMER",
        )
        legacy_kafka = _merge_kafka_mappings(legacy_kafka, legacy_consumer)
        kafka = _merge_kafka_mappings(legacy_kafka, kafka)
        kafka.setdefault("BOOTSTRAP_SERVERS", inherited("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        kafka.setdefault("SECURITY_PROTOCOL", inherited("KAFKA_SECURITY_PROTOCOL", kafka.get("SECURITY_PROTOCOL", "PLAINTEXT")))
        kafka.setdefault("SASL_MECHANISM", inherited("KAFKA_SASL_MECHANISM", kafka.get("SASL_MECHANISM")))

        username = inherited("KAFKA_USERNAME", inherited("KAFKA_SASL_USERNAME", kafka.get("USERNAME")))
        if not username:
            username_file = inherited("KAFKA_USERNAME_FILE", None)
            if username_file:
                username = _read_text_file(username_file, name="KAFKA_USERNAME_FILE")
        kafka.setdefault("USERNAME", username)

        password = inherited("KAFKA_PASSWORD", inherited("KAFKA_SASL_PASSWORD", kafka.get("PASSWORD")))
        if not password:
            password_file = inherited("KAFKA_PASSWORD_FILE", None)
            if password_file:
                password = _read_text_file(password_file, name="KAFKA_PASSWORD_FILE")
        kafka.setdefault("PASSWORD", password)
        kafka.setdefault("SSL_CA_LOCATION", inherited("KAFKA_CA_FILE", inherited("KAFKA_SSL_CA_LOCATION", kafka.get("SSL_CA_LOCATION"))))
        merged["KAFKA"] = kafka

        nats = dict(merged.get("NATS") or {})
        nats.setdefault("SERVERS", inherited("NATS_SERVERS", inherited("NATS_URL", "nats://localhost:4222")))
        nats.setdefault("TOKEN", inherited("NATS_TOKEN", None))
        nats.setdefault("CREDENTIALS_FILE", inherited("NATS_CRED_FILE", None))
        nats.setdefault("TLS_CA_FILE", inherited("NATS_CA_FILE", inherited("NATS_TLS_CA", None)))
        nats.setdefault("JETSTREAM", inherited("NATS_USE_JETSTREAM", True))
        merged["NATS"] = nats
        return cls.from_mapping(merged)
