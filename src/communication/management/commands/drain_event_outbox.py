from __future__ import annotations

import asyncio
from typing import Any

from django.core.management.base import BaseCommand

from communication.config import CommunicationSettings
from communication.outbox import OutboxService


class Command(BaseCommand):
    help = "Publish ready transactional-outbox events."

    def add_arguments(self, parser):
        parser.add_argument("--batch-size", type=int, default=100)
        parser.add_argument("--max-attempts", type=int, default=20)
        parser.add_argument("--loop", action="store_true")
        parser.add_argument("--interval", type=float, default=1.0)

    def handle(self, *args: Any, **options: Any) -> None:
        async def run() -> None:
            service = OutboxService(CommunicationSettings.from_django_settings())
            try:
                while True:
                    count = await service.publish_batch(
                        batch_size=options["batch_size"],
                        max_attempts=options["max_attempts"],
                    )
                    self.stdout.write(f"Published {count} outbox event(s).")
                    if not options["loop"]:
                        break
                    await asyncio.sleep(max(0.1, options["interval"]))
            finally:
                await service.close()

        asyncio.run(run())
