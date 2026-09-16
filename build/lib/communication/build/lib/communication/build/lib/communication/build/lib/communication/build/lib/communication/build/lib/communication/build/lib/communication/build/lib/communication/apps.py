from __future__ import annotations

from django.apps import AppConfig


class CommunicationConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "communication"
    verbose_name = "Communication"

    def ready(self) -> None:
        from django.core.checks import register

        from .checks import communication_checks

        register()(communication_checks)
