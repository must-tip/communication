from __future__ import annotations

import pytest

pytest.importorskip("django")

# Database behavior is covered in the host project where Django settings and
# migrations are available. This file ensures the models remain importable.

def test_outbox_models_have_required_uniqueness():
    from communication.models import InboxEvent, OutboxEvent

    assert OutboxEvent._meta.get_field("event_id").unique is True
    assert any(constraint.name == "comm_inbox_group_event_uniq" for constraint in InboxEvent._meta.constraints)
