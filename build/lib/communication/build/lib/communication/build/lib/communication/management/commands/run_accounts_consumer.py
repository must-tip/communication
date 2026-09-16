from __future__ import annotations

import importlib
import logging
import signal
import threading
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from communication.consumer import create_consumer_service, run_consumer_once

logger = logging.getLogger(__name__)


def _import_handler(path: str):
    module_name, separator, attribute = path.rpartition(".")
    if not separator:
        raise CommandError("--handler must be a dotted callable path")
    try:
        callback = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError) as exc:
        raise CommandError("Could not import the configured handler") from exc
    if not callable(callback):
        raise CommandError("The configured handler is not callable")
    return callback


class Command(BaseCommand):
    help = "Run an accounts event consumer over Kafka, NATS, or both."

    def add_arguments(self, parser):
        parser.add_argument("--topic", action="append", dest="topics", default=[])
        parser.add_argument("--group-id", default="accounts-events")
        parser.add_argument("--handler", required=True)
        parser.add_argument("--transport", action="append", dest="transports", choices=("kafka", "nats"))
        parser.add_argument("--concurrency", type=int, default=4)
        parser.add_argument("--max-pending", type=int, default=2000)
        parser.add_argument("--once", action="store_true")
        parser.add_argument("--timeout", type=float, default=10.0)

    def handle(self, *args: Any, **options: Any) -> None:
        topics = options["topics"] or ["accounts.user.created"]
        handler = _import_handler(options["handler"])
        transports = tuple(options["transports"] or ()) or None
        if options["once"]:
            result = run_consumer_once(
                topics,
                options["group_id"],
                user_handler=handler,
                concurrency=options["concurrency"],
                timeout=options["timeout"],
                transports=transports,
            )
            if result is None:
                self.stdout.write(self.style.WARNING("No event was consumed before the timeout."))
            else:
                self.stdout.write(self.style.SUCCESS("Consumed one event successfully."))
            return

        consumer = create_consumer_service(
            topics,
            options["group_id"],
            handler=handler,
            concurrency=options["concurrency"],
            transports=transports,
        )
        stop = threading.Event()

        def request_stop(_signum: int, _frame: object) -> None:
            stop.set()

        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)
        thread = consumer.start(handler, max_pending_tasks=options["max_pending"])
        self.stdout.write(self.style.SUCCESS("Consumer started."))
        try:
            while not stop.wait(0.5):
                if not thread.is_alive():
                    raise CommandError("Consumer thread exited unexpectedly")
        finally:
            consumer.stop(timeout=30.0)
            thread.join(timeout=5.0)
