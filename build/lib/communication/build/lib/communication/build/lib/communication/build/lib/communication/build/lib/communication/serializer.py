"""Canonical serialization adapters for the Must Tip communication system.

Kafka and NATS both transport bytes. Serialization is therefore a communication
contract concern, not a broker-specific concern. This module delegates event
validation and canonical encoding to the existing ``EventEnvelope`` implementation
so there is only one wire format and one validation policy.

The public callables accept optional context arguments for compatibility with
Kafka/NATS client callbacks and the existing legacy consumer deserializer API.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping, Protocol, TypeAlias

from .envelope import EventEnvelope
from .errors import EnvelopeValidationError

BytesLike: TypeAlias = bytes | bytearray | memoryview
RawMessage: TypeAlias = BytesLike | str
SerializationContext: TypeAlias = Mapping[str, object] | object | None

EVENT_CONTENT_TYPE = "application/vnd.musttip.event+json"
JSON_CONTENT_TYPE = "application/json"
UTF8 = "utf-8"


class Serializer(Protocol):
    def serialize(self, value: object, context: SerializationContext = None) -> bytes: ...


class Deserializer(Protocol):
    def deserialize(self, value: RawMessage, context: SerializationContext = None) -> object: ...


def _coerce_bytes(value: RawMessage, *, maximum_bytes: int, field_name: str) -> bytes:
    if isinstance(value, str):
        try:
            encoded = value.encode(UTF8, "strict")
        except UnicodeError as exc:
            raise EnvelopeValidationError(f"{field_name} is not valid UTF-8 text.") from exc
    elif isinstance(value, bytes):
        encoded = value
    elif isinstance(value, (bytearray, memoryview)):
        encoded = bytes(value)
    else:
        raise EnvelopeValidationError(
            f"{field_name} must be bytes, bytearray, memoryview, or string."
        )

    if not encoded:
        raise EnvelopeValidationError(f"{field_name} cannot be empty.")
    if len(encoded) > maximum_bytes:
        raise EnvelopeValidationError(f"{field_name} exceeds the configured maximum size.")
    return encoded


def _context_headers(context: SerializationContext) -> Mapping[str, object]:
    if isinstance(context, Mapping):
        headers = context.get("headers")
        if isinstance(headers, Mapping):
            return headers
        return context
    headers = getattr(context, "headers", None)
    return headers if isinstance(headers, Mapping) else {}


def _header_value(headers: Mapping[str, object], name: str) -> str | None:
    expected = name.casefold()
    for raw_name, raw_value in headers.items():
        if str(raw_name).strip().casefold() != expected:
            continue
        if isinstance(raw_value, bytes):
            try:
                return raw_value.decode(UTF8, "strict").strip()
            except UnicodeError:
                return None
        if raw_value is None:
            return None
        return str(raw_value).strip()
    return None


def _raise_invalid_constant(value: str) -> object:
    raise EnvelopeValidationError(f"Non-finite JSON number {value!r} is not permitted.")


def _strict_json_loads(payload: bytes) -> object:
    try:
        text = payload.decode(UTF8, "strict")
    except UnicodeError as exc:
        raise EnvelopeValidationError("Message payload is not valid UTF-8.") from exc
    try:
        return json.loads(text, parse_constant=_raise_invalid_constant)
    except EnvelopeValidationError:
        raise
    except (json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise EnvelopeValidationError("Message payload is not valid JSON.") from exc


def _validate_json_shape(
    value: object,
    *,
    maximum_depth: int,
    maximum_nodes: int,
    maximum_string_length: int,
) -> object:
    nodes = 0
    stack: list[tuple[object, int]] = [(value, 0)]
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > maximum_nodes:
            raise EnvelopeValidationError("JSON payload exceeds the configured node limit.")
        if depth > maximum_depth:
            raise EnvelopeValidationError("JSON payload exceeds the configured nesting limit.")

        if current is None or isinstance(current, (bool, int, float)):
            if isinstance(current, float) and (
                current != current or current in (float("inf"), float("-inf"))
            ):
                raise EnvelopeValidationError("Non-finite JSON numbers are not permitted.")
            continue
        if isinstance(current, str):
            if len(current) > maximum_string_length:
                raise EnvelopeValidationError("JSON string exceeds the configured maximum length.")
            continue
        if isinstance(current, list):
            stack.extend((item, depth + 1) for item in reversed(current))
            continue
        if isinstance(current, dict):
            for key, item in current.items():
                if not isinstance(key, str):
                    raise EnvelopeValidationError("JSON object keys must be strings.")
                if len(key) > maximum_string_length:
                    raise EnvelopeValidationError(
                        "JSON object key exceeds the configured maximum length."
                    )
                stack.append((item, depth + 1))
            continue
        raise EnvelopeValidationError(
            f"Unsupported JSON value type: {type(current).__name__}."
        )
    return value


@dataclass(frozen=True, slots=True)
class EventEnvelopeSerializer:
    """Serialize and deserialize the canonical cross-broker event envelope."""

    maximum_bytes: int = 1_048_576
    enforce_content_type: bool = False

    def __post_init__(self) -> None:
        if not 1 <= int(self.maximum_bytes) <= 268_435_456:
            raise ValueError("maximum_bytes must be between 1 and 268435456.")

    def serialize(self, value: object, context: SerializationContext = None) -> bytes:
        del context
        if not isinstance(value, EventEnvelope):
            raise EnvelopeValidationError(
                "Event serializer requires an EventEnvelope instance."
            )
        return value.to_bytes(maximum_bytes=self.maximum_bytes)

    def deserialize(
        self,
        value: RawMessage,
        context: SerializationContext = None,
    ) -> EventEnvelope:
        payload = _coerce_bytes(
            value,
            maximum_bytes=self.maximum_bytes,
            field_name="event payload",
        )
        if self.enforce_content_type:
            content_type = _header_value(_context_headers(context), "content-type")
            if content_type:
                media_type = content_type.split(";", 1)[0].strip().casefold()
                if media_type != EVENT_CONTENT_TYPE:
                    raise EnvelopeValidationError(
                        "Message content type is not the Must Tip event format."
                    )
        return EventEnvelope.from_bytes(payload, maximum_bytes=self.maximum_bytes)

    def __call__(self, value: object, context: SerializationContext = None) -> bytes:
        return self.serialize(value, context)


@dataclass(frozen=True, slots=True)
class JsonPayloadSerializer:
    """Strict JSON adapter for existing payload-only integrations."""

    maximum_bytes: int = 1_048_576
    maximum_depth: int = 32
    maximum_nodes: int = 50_000
    maximum_string_length: int = 262_144

    def __post_init__(self) -> None:
        if not 1 <= int(self.maximum_bytes) <= 268_435_456:
            raise ValueError("maximum_bytes must be between 1 and 268435456.")
        if not 1 <= int(self.maximum_depth) <= 128:
            raise ValueError("maximum_depth must be between 1 and 128.")
        if not 1 <= int(self.maximum_nodes) <= 1_000_000:
            raise ValueError("maximum_nodes must be between 1 and 1000000.")
        if not 1 <= int(self.maximum_string_length) <= 4_194_304:
            raise ValueError("maximum_string_length must be between 1 and 4194304.")

    def serialize(self, value: object, context: SerializationContext = None) -> bytes:
        del context
        _validate_json_shape(
            value,
            maximum_depth=self.maximum_depth,
            maximum_nodes=self.maximum_nodes,
            maximum_string_length=self.maximum_string_length,
        )
        try:
            payload = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode(UTF8, "strict")
        except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
            raise EnvelopeValidationError(
                "Payload cannot be serialized as strict JSON."
            ) from exc
        if len(payload) > self.maximum_bytes:
            raise EnvelopeValidationError(
                "Serialized payload exceeds the configured maximum size."
            )
        return payload

    def deserialize(
        self,
        value: RawMessage,
        context: SerializationContext = None,
    ) -> object:
        del context
        payload = _coerce_bytes(
            value,
            maximum_bytes=self.maximum_bytes,
            field_name="JSON payload",
        )
        decoded = _strict_json_loads(payload)
        return _validate_json_shape(
            decoded,
            maximum_depth=self.maximum_depth,
            maximum_nodes=self.maximum_nodes,
            maximum_string_length=self.maximum_string_length,
        )

    def __call__(self, value: object, context: SerializationContext = None) -> bytes:
        return self.serialize(value, context)


DEFAULT_EVENT_SERIALIZER = EventEnvelopeSerializer()
DEFAULT_JSON_SERIALIZER = JsonPayloadSerializer()


def serialize_event(
    value: EventEnvelope,
    context: SerializationContext = None,
    *,
    maximum_bytes: int = 1_048_576,
) -> bytes:
    return EventEnvelopeSerializer(maximum_bytes=maximum_bytes).serialize(value, context)


def deserialize_event(
    value: RawMessage,
    context: SerializationContext = None,
    *,
    maximum_bytes: int = 1_048_576,
    enforce_content_type: bool = False,
) -> EventEnvelope:
    return EventEnvelopeSerializer(
        maximum_bytes=maximum_bytes,
        enforce_content_type=enforce_content_type,
    ).deserialize(value, context)


def serialize_payload(
    value: object,
    context: SerializationContext = None,
    *,
    maximum_bytes: int = 1_048_576,
) -> bytes:
    return JsonPayloadSerializer(maximum_bytes=maximum_bytes).serialize(value, context)


def deserialize_payload(
    value: RawMessage,
    context: SerializationContext = None,
    *,
    maximum_bytes: int = 1_048_576,
) -> object:
    return JsonPayloadSerializer(maximum_bytes=maximum_bytes).deserialize(value, context)


json_serializer = serialize_payload
json_deserializer = deserialize_payload
event_serializer = serialize_event
event_deserializer = deserialize_event

__all__ = [
    "DEFAULT_EVENT_SERIALIZER",
    "DEFAULT_JSON_SERIALIZER",
    "EVENT_CONTENT_TYPE",
    "EventEnvelopeSerializer",
    "JSON_CONTENT_TYPE",
    "JsonPayloadSerializer",
    "deserialize_event",
    "deserialize_payload",
    "event_deserializer",
    "event_serializer",
    "json_deserializer",
    "json_serializer",
    "serialize_event",
    "serialize_payload",
]
