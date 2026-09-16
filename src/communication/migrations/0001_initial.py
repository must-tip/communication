from __future__ import annotations

import uuid

from django.db import migrations, models
from django.db.models import Q
import django.utils.timezone


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="InboxEvent",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("consumer_group", models.CharField(max_length=255)),
                ("event_id", models.UUIDField()),
                ("status", models.CharField(choices=[("processing", "Processing"), ("complete", "Complete")], default="processing", max_length=16)),
                ("lease_expires_at", models.DateTimeField()),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name="OutboxEvent",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("event_id", models.UUIDField(db_index=True, unique=True)),
                ("event_type", models.CharField(db_index=True, max_length=255)),
                ("topic", models.CharField(db_index=True, max_length=255)),
                ("envelope", models.JSONField()),
                ("target_transports", models.JSONField(default=list)),
                ("delivered_transports", models.JSONField(default=list)),
                ("status", models.CharField(choices=[("pending", "Pending"), ("processing", "Processing"), ("published", "Published"), ("failed", "Failed")], db_index=True, default="pending", max_length=16)),
                ("attempts", models.PositiveIntegerField(default=0)),
                ("available_at", models.DateTimeField(db_index=True, default=django.utils.timezone.now)),
                ("locked_at", models.DateTimeField(blank=True, null=True)),
                ("published_at", models.DateTimeField(blank=True, null=True)),
                ("last_error_code", models.CharField(blank=True, default="", max_length=128)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={"ordering": ("created_at",)},
        ),
        migrations.AddConstraint(
            model_name="inboxevent",
            constraint=models.UniqueConstraint(fields=("consumer_group", "event_id"), name="comm_inbox_group_event_uniq"),
        ),
        migrations.AddIndex(model_name="inboxevent", index=models.Index(fields=["consumer_group", "status"], name="comm_inbox_group_status_idx")),
        migrations.AddIndex(model_name="inboxevent", index=models.Index(fields=["expires_at"], name="comm_inbox_expiry_idx")),
        migrations.AddConstraint(
            model_name="outboxevent",
            constraint=models.CheckConstraint(condition=Q(attempts__gte=0), name="comm_outbox_attempts_nonneg"),
        ),
        migrations.AddIndex(model_name="outboxevent", index=models.Index(fields=["status", "available_at"], name="comm_outbox_ready_idx")),
        migrations.AddIndex(model_name="outboxevent", index=models.Index(fields=["event_type", "created_at"], name="comm_outbox_type_idx")),
    ]
