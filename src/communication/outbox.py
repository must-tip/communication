from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import timedelta
from typing import Sequence

from .bus import EventBus
from .config import CommunicationSettings
from .envelope import EventEnvelope

logger = logging.getLogger(__name__)


class OutboxService:
    def __init__(self, settings: CommunicationSettings, *, event_bus: EventBus | None = None) -> None:
        self.settings = settings
        self.bus = event_bus or EventBus(settings)

    def enqueue(
        self,
        event: EventEnvelope,
        *,
        transports: Sequence[str] | None = None,
        using: str = "default",
    ):
        from .models import OutboxEvent

        targets = list(dict.fromkeys(transports or self.settings.enabled_transports))
        return OutboxEvent.objects.using(using).create(
            event_id=event.event_id,
            event_type=event.event_type,
            topic=event.topic,
            envelope=event.as_dict(),
            target_transports=targets,
            delivered_transports=[],
        )

    def enqueue_on_commit(
        self,
        event: EventEnvelope,
        *,
        transports: Sequence[str] | None = None,
        using: str = "default",
    ) -> None:
        """Persist the outbox row in the caller's current database transaction.

        The public name is retained for compatibility. Deferring the INSERT to
        ``transaction.on_commit`` created a crash window after the business
        transaction committed but before the outbox callback ran. Writing now
        makes the business change and outbox row commit or roll back together.
        """

        self.enqueue(event, transports=transports, using=using)

    @staticmethod
    def _claim_batch_sync(*, batch_size: int, max_attempts: int, lease_seconds: int, using: str):
        from django.db import transaction
        from django.utils import timezone

        from .models import OutboxEvent

        with transaction.atomic(using=using):
            now = timezone.now()
            OutboxEvent.objects.using(using).filter(
                status=OutboxEvent.Status.PROCESSING,
                locked_at__lt=now - timedelta(seconds=lease_seconds),
            ).update(
                status=OutboxEvent.Status.FAILED,
                locked_at=None,
                available_at=now,
                last_error_code="lease_expired",
                updated_at=now,
            )
            rows = list(
                OutboxEvent.objects.using(using)
                .select_for_update(skip_locked=True)
                .ready()
                .filter(attempts__lt=max_attempts)
                .order_by("available_at", "created_at")[:batch_size]
            )
            for row in rows:
                row.status = OutboxEvent.Status.PROCESSING
                row.locked_at = now
                row.attempts += 1
                row.updated_at = now
            if rows:
                OutboxEvent.objects.using(using).bulk_update(rows, ("status", "locked_at", "attempts", "updated_at"))
            return rows

    @staticmethod
    def _save_success_sync(row_id, delivered: list[str], *, complete: bool, using: str) -> None:
        from django.utils import timezone

        from .models import OutboxEvent

        now = timezone.now()
        updates = {
            "delivered_transports": delivered,
            "last_error_code": "",
            "updated_at": now,
        }
        if complete:
            updates.update(
                status=OutboxEvent.Status.PUBLISHED,
                published_at=now,
                locked_at=None,
            )
        else:
            # Keep the row leased by this worker while remaining transports are
            # published. Exposing it as pending here would allow a second worker
            # to claim the same event concurrently.
            updates.update(status=OutboxEvent.Status.PROCESSING)
        OutboxEvent.objects.using(using).filter(pk=row_id).update(**updates)

    @staticmethod
    def _save_failure_sync(row_id, delivered: list[str], error_code: str, delay_seconds: float, *, using: str) -> None:
        from django.utils import timezone

        from .models import OutboxEvent

        OutboxEvent.objects.using(using).filter(pk=row_id).update(
            delivered_transports=delivered,
            status=OutboxEvent.Status.FAILED,
            locked_at=None,
            last_error_code=error_code[:128],
            available_at=timezone.now() + timedelta(seconds=max(0.1, delay_seconds)),
            updated_at=timezone.now(),
        )

    async def publish_batch(
        self,
        *,
        batch_size: int = 100,
        max_attempts: int = 20,
        lease_seconds: int = 300,
        using: str = "default",
    ) -> int:
        rows = await asyncio.to_thread(
            self._claim_batch_sync,
            batch_size=max(1, min(int(batch_size), 1000)),
            max_attempts=max(1, int(max_attempts)),
            lease_seconds=max(30, int(lease_seconds)),
            using=using,
        )
        published = 0
        for row in rows:
            delivered = list(dict.fromkeys(row.delivered_transports or []))
            try:
                payload = json.dumps(row.envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                event = EventEnvelope.from_bytes(payload, maximum_bytes=self.settings.max_event_bytes)
                for transport in row.target_transports:
                    if transport in delivered:
                        continue
                    await self.bus.publish(event, mode="primary", transports=(transport,), key=event.event_id)
                    delivered.append(transport)
                    await asyncio.to_thread(
                        self._save_success_sync,
                        row.pk,
                        delivered,
                        complete=False,
                        using=using,
                    )
                await asyncio.to_thread(
                    self._save_success_sync,
                    row.pk,
                    delivered,
                    complete=set(delivered) >= set(row.target_transports),
                    using=using,
                )
                published += 1
            except Exception as exc:
                delay = min(3600.0, 0.5 * (2 ** min(row.attempts, 12)) + random.random())
                await asyncio.to_thread(
                    self._save_failure_sync,
                    row.pk,
                    delivered,
                    type(exc).__name__,
                    delay,
                    using=using,
                )
                logger.exception("outbox publication failed", extra={"event_id": str(row.event_id)})
        return published

    async def close(self) -> None:
        await self.bus.close()
