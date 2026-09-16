from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from enum import Enum
from typing import Protocol

from .errors import ConfigurationError

logger = logging.getLogger(__name__)


class ClaimState(str, Enum):
    CLAIMED = "claimed"
    PROCESSING = "processing"
    COMPLETE = "complete"


class IdempotencyStore(Protocol):
    async def claim(self, key: str, ttl_seconds: int) -> ClaimState:
        ...

    async def complete(self, key: str, ttl_seconds: int) -> None:
        ...

    async def release(self, key: str) -> None:
        ...


class MemoryIdempotencyStore:
    """Process-local TTL store intended for tests and single-process development."""

    def __init__(self, *, maximum_entries: int = 100_000) -> None:
        self._maximum_entries = max(100, int(maximum_entries))
        self._items: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def claim(self, key: str, ttl_seconds: int) -> ClaimState:
        now = time.monotonic()
        async with self._lock:
            expired = [item for item, (_, deadline) in self._items.items() if deadline <= now]
            for item in expired:
                self._items.pop(item, None)
            existing = self._items.get(key)
            if existing is not None:
                return ClaimState.COMPLETE if existing[0] == "complete" else ClaimState.PROCESSING
            self._items[key] = ("processing", now + max(1, ttl_seconds))
            self._items.move_to_end(key)
            while len(self._items) > self._maximum_entries:
                self._items.popitem(last=False)
            return ClaimState.CLAIMED

    async def complete(self, key: str, ttl_seconds: int) -> None:
        async with self._lock:
            self._items[key] = ("complete", time.monotonic() + max(1, ttl_seconds))
            self._items.move_to_end(key)

    async def release(self, key: str) -> None:
        async with self._lock:
            self._items.pop(key, None)


class DjangoCacheIdempotencyStore:
    """Shared cache-backed store suitable for multi-process consumers."""

    def __init__(self, *, prefix: str = "communication:idempotency", fail_closed: bool = True) -> None:
        self.prefix = prefix
        self.fail_closed = bool(fail_closed)

    def _key(self, value: str) -> str:
        return f"{self.prefix}:{value}"

    async def claim(self, key: str, ttl_seconds: int) -> ClaimState:
        try:
            from django.core.cache import cache

            cache_key = self._key(key)
            added = await asyncio.to_thread(cache.add, cache_key, "processing", ttl_seconds)
            if added:
                return ClaimState.CLAIMED
            existing = await asyncio.to_thread(cache.get, cache_key)
            return ClaimState.COMPLETE if existing == "complete" else ClaimState.PROCESSING
        except Exception:
            logger.exception("idempotency cache claim failed")
            return ClaimState.PROCESSING if self.fail_closed else ClaimState.CLAIMED

    async def complete(self, key: str, ttl_seconds: int) -> None:
        try:
            from django.core.cache import cache

            await asyncio.to_thread(cache.set, self._key(key), "complete", ttl_seconds)
        except Exception:
            logger.exception("idempotency cache completion failed")
            if self.fail_closed:
                raise

    async def release(self, key: str) -> None:
        try:
            from django.core.cache import cache

            await asyncio.to_thread(cache.delete, self._key(key))
        except Exception:
            logger.exception("idempotency cache release failed")
            if self.fail_closed:
                raise


class DjangoModelIdempotencyStore:
    """Database-backed inbox with a processing lease and completion TTL."""

    def __init__(self, *, consumer_group: str, lease_seconds: int = 300, using: str = "default") -> None:
        self.consumer_group = consumer_group
        self.lease_seconds = max(30, int(lease_seconds))
        self.using = using

    def _split(self, key: str) -> str:
        return key.rsplit(":", 1)[-1]

    def _claim_sync(self, key: str, ttl_seconds: int) -> ClaimState:
        from datetime import timedelta

        from django.db import IntegrityError, transaction
        from django.utils import timezone

        from .models import InboxEvent

        now = timezone.now()
        event_id = self._split(key)
        with transaction.atomic(using=self.using):
            try:
                row = InboxEvent.objects.using(self.using).select_for_update().get(
                    consumer_group=self.consumer_group, event_id=event_id
                )
            except InboxEvent.DoesNotExist:
                try:
                    InboxEvent.objects.using(self.using).create(
                        consumer_group=self.consumer_group,
                        event_id=event_id,
                        status=InboxEvent.Status.PROCESSING,
                        lease_expires_at=now + timedelta(seconds=self.lease_seconds),
                        expires_at=now + timedelta(seconds=ttl_seconds),
                    )
                    return ClaimState.CLAIMED
                except IntegrityError:
                    row = InboxEvent.objects.using(self.using).select_for_update().get(
                        consumer_group=self.consumer_group, event_id=event_id
                    )
            if row.status == InboxEvent.Status.COMPLETE and row.expires_at > now:
                return ClaimState.COMPLETE
            if row.status == InboxEvent.Status.PROCESSING and row.lease_expires_at > now:
                return ClaimState.PROCESSING
            row.status = InboxEvent.Status.PROCESSING
            row.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            row.expires_at = now + timedelta(seconds=ttl_seconds)
            row.save(update_fields=("status", "lease_expires_at", "expires_at", "updated_at"))
            return ClaimState.CLAIMED

    async def claim(self, key: str, ttl_seconds: int) -> ClaimState:
        return await asyncio.to_thread(self._claim_sync, key, ttl_seconds)

    def _complete_sync(self, key: str, ttl_seconds: int) -> None:
        from datetime import timedelta

        from django.utils import timezone

        from .models import InboxEvent

        now = timezone.now()
        InboxEvent.objects.using(self.using).filter(
            consumer_group=self.consumer_group, event_id=self._split(key)
        ).update(
            status=InboxEvent.Status.COMPLETE,
            lease_expires_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
            updated_at=now,
        )

    async def complete(self, key: str, ttl_seconds: int) -> None:
        await asyncio.to_thread(self._complete_sync, key, ttl_seconds)

    async def release(self, key: str) -> None:
        from .models import InboxEvent

        await asyncio.to_thread(
            InboxEvent.objects.using(self.using).filter(
                consumer_group=self.consumer_group,
                event_id=self._split(key),
                status=InboxEvent.Status.PROCESSING,
            ).delete
        )


def build_idempotency_store(*, backend: str = "django-cache", fail_closed: bool = True, consumer_group: str = "default") -> IdempotencyStore:
    normalized = backend.strip().casefold()
    if normalized in {"memory", "local"}:
        return MemoryIdempotencyStore()
    if normalized in {"django", "django-cache", "cache"}:
        return DjangoCacheIdempotencyStore(fail_closed=fail_closed)
    if normalized in {"database", "db", "django-model"}:
        return DjangoModelIdempotencyStore(consumer_group=consumer_group)
    raise ConfigurationError(f"unknown idempotency backend: {backend}")
