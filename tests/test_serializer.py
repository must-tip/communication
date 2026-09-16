from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from communication.envelope import EventEnvelope
from communication.errors import EnvelopeValidationError
from communication.serializer import (
    EVENT_CONTENT_TYPE,
    EventEnvelopeSerializer,
    JsonPayloadSerializer,
    deserialize_event,
    deserialize_payload,
    serialize_event,
    serialize_payload,
)


def test_event_serializer_delegates_to_existing_envelope_contract() -> None:
    event = EventEnvelope(
        event_type="accounts.user.created",
        source="authentication",
        topic="accounts.user.created",
        data={
            "amount": Decimal("10.50"),
            "when": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
    )
    decoded = deserialize_event(serialize_event(event))
    assert decoded.event_id == event.event_id
    assert decoded.event_type == event.event_type
    assert decoded.data == {
        "amount": "10.50",
        "when": "2026-01-01T00:00:00.000Z",
    }


def test_event_serializer_rejects_non_envelope_values() -> None:
    with pytest.raises(EnvelopeValidationError):
        EventEnvelopeSerializer().serialize({"event_type": "bad"})


def test_event_deserializer_accepts_supported_wire_inputs() -> None:
    event = EventEnvelope(
        event_type="accounts.test",
        source="authentication",
        topic="accounts.test",
        data={"ok": True},
    )
    payload = event.to_bytes()
    assert deserialize_event(payload).event_id == event.event_id
    assert deserialize_event(bytearray(payload)).event_id == event.event_id
    assert deserialize_event(memoryview(payload)).event_id == event.event_id
    assert deserialize_event(payload.decode("utf-8")).event_id == event.event_id


def test_content_type_enforcement_is_optional_and_broker_agnostic() -> None:
    event = EventEnvelope(
        event_type="accounts.test",
        source="authentication",
        topic="accounts.test",
        data={},
    )
    payload = event.to_bytes()
    serializer = EventEnvelopeSerializer(enforce_content_type=True)
    assert serializer.deserialize(
        payload,
        {"headers": {"content-type": EVENT_CONTENT_TYPE}},
    ).event_id == event.event_id
    with pytest.raises(EnvelopeValidationError):
        serializer.deserialize(
            payload,
            {"headers": {"content-type": "application/octet-stream"}},
        )


def test_json_payload_serializer_is_deterministic() -> None:
    left = serialize_payload({"b": 2, "a": 1})
    right = serialize_payload({"a": 1, "b": 2})
    assert left == right == b'{"a":1,"b":2}'
    assert deserialize_payload(left) == {"a": 1, "b": 2}


@pytest.mark.parametrize(
    "value",
    [
        {"value": float("nan")},
        {"value": float("inf")},
        {"value": object()},
        {1: "non-string-key"},
    ],
)
def test_json_payload_serializer_rejects_unsafe_values(value: object) -> None:
    with pytest.raises(EnvelopeValidationError):
        serialize_payload(value)


def test_json_deserializer_rejects_nonfinite_constants() -> None:
    with pytest.raises(EnvelopeValidationError):
        deserialize_payload(b'{"value":NaN}')


def test_size_limit_is_enforced_before_json_parsing() -> None:
    serializer = JsonPayloadSerializer(maximum_bytes=8)
    with pytest.raises(EnvelopeValidationError):
        serializer.deserialize(b'{"value":12345}')


def test_legacy_deserializer_signature_accepts_metadata() -> None:
    metadata = {
        "topic": "accounts.test",
        "headers": {"content-type": "application/json"},
    }
    assert deserialize_payload(b'{"ok":true}', metadata) == {"ok": True}
