from __future__ import annotations

import asyncio


def drain_outbox_batch(batch_size: int = 100) -> int:
    from .config import CommunicationSettings
    from .outbox import OutboxService

    service = OutboxService(CommunicationSettings.from_django_settings())

    async def run() -> int:
        try:
            return await service.publish_batch(batch_size=batch_size)
        finally:
            await service.close()

    return asyncio.run(run())


try:
    from celery import shared_task
except ImportError:
    shared_task = None

if shared_task is not None:

    @shared_task(
        bind=True,
        autoretry_for=(Exception,),
        retry_backoff=True,
        retry_jitter=True,
        max_retries=8,
        acks_late=True,
    )
    def publish_communication_outbox(self, batch_size: int = 100) -> int:
        return drain_outbox_batch(batch_size=batch_size)
