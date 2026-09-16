from __future__ import annotations

import uuid
from datetime import timedelta

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import Q
from django.utils import timezone


class OutboxEventQuerySet(models.QuerySet):
    def ready(self):
        return self.filter(
            status__in=(OutboxEvent.Status.PENDING, OutboxEvent.Status.FAILED),
            available_at__lte=timezone.now(),
        )

    def stale_processing(self, *, older_than_seconds: int = 300):
        cutoff = timezone.now() - timedelta(seconds=max(1, older_than_seconds))
        return self.filter(status=OutboxEvent.Status.PROCESSING, locked_at__lt=cutoff)


class OutboxEvent(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PROCESSING = "processing", "Processing"
        PUBLISHED = "published", "Published"
        FAILED = "failed", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    event_id = models.UUIDField(unique=True, db_index=True)
    event_type = models.CharField(max_length=255, db_index=True)
    topic = models.CharField(max_length=255, db_index=True)
    envelope = models.JSONField()
    target_transports = models.JSONField(default=list)
    delivered_transports = models.JSONField(default=list)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING, db_index=True)
    attempts = models.PositiveIntegerField(default=0)
    available_at = models.DateTimeField(default=timezone.now, db_index=True)
    locked_at = models.DateTimeField(null=True, blank=True)
    published_at = models.DateTimeField(null=True, blank=True)
    last_error_code = models.CharField(max_length=128, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = OutboxEventQuerySet.as_manager()

    class Meta:
        ordering = ("created_at",)
        indexes = (
            models.Index(fields=("status", "available_at"), name="comm_outbox_ready_idx"),
            models.Index(fields=("event_type", "created_at"), name="comm_outbox_type_idx"),
        )
        constraints = (
            models.CheckConstraint(condition=Q(attempts__gte=0), name="comm_outbox_attempts_nonneg"),
        )

    def clean(self) -> None:
        if not isinstance(self.envelope, dict):
            raise ValidationError({"envelope": "Envelope must be a JSON object."})
        if not isinstance(self.target_transports, list) or not self.target_transports:
            raise ValidationError({"target_transports": "At least one target transport is required."})
        if not isinstance(self.delivered_transports, list):
            raise ValidationError({"delivered_transports": "Delivered transports must be a list."})


class InboxEvent(models.Model):
    class Status(models.TextChoices):
        PROCESSING = "processing", "Processing"
        COMPLETE = "complete", "Complete"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    consumer_group = models.CharField(max_length=255)
    event_id = models.UUIDField()
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PROCESSING)
    lease_expires_at = models.DateTimeField()
    expires_at = models.DateTimeField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = (
            models.UniqueConstraint(fields=("consumer_group", "event_id"), name="comm_inbox_group_event_uniq"),
        )
        indexes = (
            models.Index(fields=("consumer_group", "status"), name="comm_inbox_group_status_idx"),
            models.Index(fields=("expires_at",), name="comm_inbox_expiry_idx"),
        )
