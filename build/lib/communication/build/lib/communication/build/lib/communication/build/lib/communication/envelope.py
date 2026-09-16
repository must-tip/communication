from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import re
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from types import MappingProxyType
from typing import Mapping, MutableMapping

from .errors import EnvelopeValidationError, SignatureError

_EVENT_NAME_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,253}[A-Za-z0-9])?$", re.ASCII)
_HEADER_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$", re.ASCII)
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_RESERVED_HEADERS = frozenset(
    {
        "content-type",
        "event-id",
        "event-type",
        "event-version",
        "event-source",
        "trace-id",
        "correlation-id",
        "causation-id",
        "tenant-id",
        "signature",
        "signature-key-id",
    }
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _rfc3339(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise EnvelopeValidationError("event timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_rfc3339(value: object) -> datetime:
    if not isinstance(value, str) or len(value) > 64 or _CONTROL_RE.search(value):
        raise EnvelopeValidationError("invalid event timestamp")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise EnvelopeValidationError("invalid event timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EnvelopeValidationError("event timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _safe_name(value: object, field_name: str, *, maximum_length: int = 255) -> str:
    if not isinstance(value, str):
        raise EnvelopeValidationError(f"{field_name} must be a string")
    candidate = value.strip()
    if (
        not candidate
        or len(candidate) > maximum_length
        or _CONTROL_RE.search(candidate)
        or _EVENT_NAME_RE.fullmatch(candidate) is None
    ):
        raise EnvelopeValidationError(f"{field_name} is invalid")
    return candidate


def _safe_optional_text(value: object, field_name: str, *, maximum_length: int = 255) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise EnvelopeValidationError(f"{field_name} must be a string")
    candidate = value.strip()
    if not candidate or len(candidate) > maximum_length or _CONTROL_RE.search(candidate):
        raise EnvelopeValidationError(f"{field_name} is invalid")
    return candidate


def _json_default(value: object) -> object:
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime):
            return _rfc3339(value)
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise TypeError("non-finite Decimal values are not supported")
        return format(value, "f")
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"unsupported event value: {type(value).__name__}")


def _validate_json_tree(
    value: object,
    *,
    maximum_depth: int,
    maximum_nodes: int,
    maximum_string_length: int,
) -> None:
    nodes = 0

    def visit(item: object, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > maximum_nodes:
            raise EnvelopeValidationError("event payload contains too many values")
        if depth > maximum_depth:
            raise EnvelopeValidationError("event payload is nested too deeply")
        if item is None or isinstance(item, (bool, int)):
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise EnvelopeValidationError("non-finite numbers are not supported")
            return
        if isinstance(item, (Decimal, uuid.UUID, date, datetime, bytes)):
            _json_default(item)
            return
        if isinstance(item, str):
            if len(item) > maximum_string_length or "\x00" in item:
                raise EnvelopeValidationError("event payload contains an invalid string")
            return
        if isinstance(item, Mapping):
            for key, nested in item.items():
                if not isinstance(key, str) or not key or len(key) > 255 or _CONTROL_RE.search(key):
                    raise EnvelopeValidationError("event payload contains an invalid object key")
                visit(nested, depth + 1)
            return
        if isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested, depth + 1)
            return
        raise EnvelopeValidationError(f"unsupported event value: {type(item).__name__}")

    visit(value, 0)


def normalize_headers(
    headers: Mapping[str, object] | None,
    *,
    maximum_headers: int = 64,
    maximum_value_length: int = 2048,
    permit_reserved: bool = False,
) -> Mapping[str, str]:
    if headers is None:
        return MappingProxyType({})
    if not isinstance(headers, Mapping) or len(headers) > maximum_headers:
        raise EnvelopeValidationError("event headers are invalid")
    normalized: dict[str, str] = {}
    for raw_name, raw_value in headers.items():
        if not isinstance(raw_name, str):
            raise EnvelopeValidationError("event header names must be strings")
        name = raw_name.strip().casefold()
        if _HEADER_NAME_RE.fullmatch(name) is None:
            raise EnvelopeValidationError("event header name is invalid")
        if not permit_reserved and name in _RESERVED_HEADERS:
            raise EnvelopeValidationError(f"event header {name!r} is reserved")
        if not isinstance(raw_value, (str, int, float, bool, uuid.UUID)):
            raise EnvelopeValidationError("event header values must be scalar")
        value = str(raw_value).strip()
        if not value or len(value) > maximum_value_length or _CONTROL_RE.search(value):
            raise EnvelopeValidationError("event header value is invalid")
        normalized[name] = value
    return MappingProxyType(normalized)


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_type: str
    data: object
    source: str
    topic: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "v1"
    occurred_at: datetime = field(default_factory=utc_now)
    trace_id: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    tenant_id: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            canonical_id = str(uuid.UUID(str(self.event_id)))
        except (ValueError, TypeError, AttributeError) as exc:
            raise EnvelopeValidationError("event_id must be a UUID") from exc
        object.__setattr__(self, "event_id", canonical_id)
        object.__setattr__(self, "event_type", _safe_name(self.event_type, "event_type"))
        object.__setattr__(self, "source", _safe_name(self.source, "source"))
        object.__setattr__(self, "topic", _safe_name(self.topic, "topic"))
        object.__setattr__(self, "schema_version", _safe_name(self.schema_version, "schema_version", maximum_length=32))
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise EnvelopeValidationError("occurred_at must be timezone-aware")
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(timezone.utc))
        object.__setattr__(self, "trace_id", _safe_optional_text(self.trace_id, "trace_id", maximum_length=128))
        object.__setattr__(self, "correlation_id", _safe_optional_text(self.correlation_id, "correlation_id", maximum_length=128))
        object.__setattr__(self, "causation_id", _safe_optional_text(self.causation_id, "causation_id", maximum_length=128))
        object.__setattr__(self, "tenant_id", _safe_optional_text(self.tenant_id, "tenant_id", maximum_length=128))
        object.__setattr__(self, "headers", normalize_headers(self.headers))

    def with_topic(self, topic: str) -> "EventEnvelope":
        return replace(self, topic=topic)

    def as_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "schema_version": self.schema_version,
            "source": self.source,
            "topic": self.topic,
            "occurred_at": _rfc3339(self.occurred_at),
            "trace_id": self.trace_id,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "tenant_id": self.tenant_id,
            "headers": dict(self.headers),
            "data": self.data,
        }

    def to_bytes(
        self,
        *,
        maximum_bytes: int = 1_048_576,
        maximum_depth: int = 20,
        maximum_nodes: int = 50_000,
        maximum_string_length: int = 262_144,
    ) -> bytes:
        _validate_json_tree(
            self.data,
            maximum_depth=maximum_depth,
            maximum_nodes=maximum_nodes,
            maximum_string_length=maximum_string_length,
        )
        try:
            payload = json.dumps(
                self.as_dict(),
                default=_json_default,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError) as exc:
            raise EnvelopeValidationError("event could not be serialized") from exc
        if len(payload) > maximum_bytes:
            raise EnvelopeValidationError(f"event exceeds maximum size of {maximum_bytes} bytes")
        return payload

    @classmethod
    def from_bytes(
        cls,
        raw: bytes | bytearray | memoryview | str,
        *,
        maximum_bytes: int = 1_048_576,
        maximum_depth: int = 20,
        maximum_nodes: int = 50_000,
        maximum_string_length: int = 262_144,
    ) -> "EventEnvelope":
        if isinstance(raw, str):
            encoded = raw.encode("utf-8")
        elif isinstance(raw, (bytes, bytearray, memoryview)):
            encoded = bytes(raw)
        else:
            raise EnvelopeValidationError("event payload must be bytes or text")
        if not encoded or len(encoded) > maximum_bytes:
            raise EnvelopeValidationError("event payload size is invalid")
        try:
            decoded = json.loads(encoded.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EnvelopeValidationError("event payload is not valid JSON") from exc
        if not isinstance(decoded, MutableMapping):
            raise EnvelopeValidationError("event envelope must be a JSON object")
        required = {"event_id", "event_type", "schema_version", "source", "topic", "occurred_at", "data"}
        if not required.issubset(decoded):
            raise EnvelopeValidationError("event envelope is missing required fields")
        _validate_json_tree(
            decoded.get("data"),
            maximum_depth=maximum_depth,
            maximum_nodes=maximum_nodes,
            maximum_string_length=maximum_string_length,
        )
        return cls(
            event_id=decoded["event_id"],
            event_type=decoded["event_type"],
            schema_version=decoded["schema_version"],
            source=decoded["source"],
            topic=decoded["topic"],
            occurred_at=_parse_rfc3339(decoded["occurred_at"]),
            trace_id=decoded.get("trace_id"),
            correlation_id=decoded.get("correlation_id"),
            causation_id=decoded.get("causation_id"),
            tenant_id=decoded.get("tenant_id"),
            headers=decoded.get("headers") or {},
            data=decoded["data"],
        )

    def transport_headers(self) -> dict[str, str]:
        values = {
            "content-type": "application/vnd.musttip.event+json",
            "event-id": self.event_id,
            "event-type": self.event_type,
            "event-version": self.schema_version,
            "event-source": self.source,
        }
        optional = {
            "trace-id": self.trace_id,
            "correlation-id": self.correlation_id,
            "causation-id": self.causation_id,
            "tenant-id": self.tenant_id,
        }
        values.update({key: value for key, value in optional.items() if value})
        values.update(self.headers)
        return values


def sign_payload(payload: bytes, *, secret: str, key_id: str) -> dict[str, str]:
    if not isinstance(secret, str) or len(secret) < 32:
        raise SignatureError("event signing secret must contain at least 32 characters")
    safe_key_id = _safe_name(key_id, "signature key id", maximum_length=64)
    digest = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).digest()
    return {
        "signature": base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii"),
        "signature-key-id": safe_key_id,
    }


def verify_payload_signature(
    payload: bytes,
    headers: Mapping[str, object],
    *,
    secrets: Mapping[str, str],
    required: bool,
) -> bool:
    signature = headers.get("signature")
    key_id = headers.get("signature-key-id")
    if signature is None and key_id is None:
        if required:
            raise SignatureError("event signature is required")
        return False
    if not isinstance(signature, str) or not isinstance(key_id, str):
        raise SignatureError("event signature headers are invalid")
    secret = secrets.get(key_id)
    if not isinstance(secret, str) or len(secret) < 32:
        raise SignatureError("event signature key is unknown")
    expected = sign_payload(payload, secret=secret, key_id=key_id)["signature"]
    if not hmac.compare_digest(expected, signature):
        raise SignatureError("event signature is invalid")
    return True
