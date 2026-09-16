"""Compatibility exports for the existing NATS notification import path.

The implementation lives in :mod:`communication.notification_sender` so legacy
callers and the transport-neutral event bus share one publication path.
"""

from __future__ import annotations

from ..notification_sender import (
    get_accounts_producer,
    get_publisher,
    publish_notification_event_async,
    publish_notification_event_sync,
)

__all__ = [
    "get_accounts_producer",
    "get_publisher",
    "publish_notification_event_async",
    "publish_notification_event_sync",
]
