from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from communication.envelope import EventEnvelope, sign_payload, verify_payload_signature
from communication.errors import EnvelopeValidationError, SignatureError


def test_envelope_round_trip_is_stable() -> None:
    event = EventEnvelope(
        event_type="accounts.user.created",
        source="authentication",
        topic="accounts.user.created",
        data={"amount": Decimal("10.50"), "when": datetime(2026, 1, 1, tzinfo=timezone.utc)},
        trace_id="trace-1",
    )
    encoded = event.to_bytes()
    decoded = EventEnvelope.from_bytes(encoded)
    assert decoded.event_id == event.event_id
    assert decoded.event_type == event.event_type
    assert decoded.data == {"amount": "10.50", "when": "2026-01-01T00:00:00.000Z"}


def test_envelope_rejects_nonfinite_and_oversized_payloads() -> None:
    with pytest.raises(EnvelopeValidationError):
        EventEnvelope(
            event_type="accounts.bad",
            source="authentication",
            topic="accounts.bad",
            data={"value": float("nan")},
        ).to_bytes()
    with pytest.raises(EnvelopeValidationError):
        EventEnvelope(
            event_type="accounts.large",
            source="authentication",
            topic="accounts.large",
            data={"value": "x" * 100},
        ).to_bytes(maximum_bytes=32)


def test_signature_is_keyed_and_verified() -> None:
    payload = b'{"event":true}'
    headers = sign_payload(payload, secret="s" * 64, key_id="key-1")
    assert verify_payload_signature(payload, headers, secrets={"key-1": "s" * 64}, required=True)
    with pytest.raises(SignatureError):
        verify_payload_signature(payload + b"x", headers, secrets={"key-1": "s" * 64}, required=True)
