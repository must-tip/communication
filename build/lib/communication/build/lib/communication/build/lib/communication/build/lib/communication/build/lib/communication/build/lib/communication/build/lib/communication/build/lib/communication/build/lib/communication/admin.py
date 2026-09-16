from __future__ import annotations

from django.contrib import admin

from .models import InboxEvent, OutboxEvent


@admin.register(OutboxEvent)
class OutboxEventAdmin(admin.ModelAdmin):
    list_display = ("event_type", "topic", "status", "attempts", "available_at", "published_at")
    list_filter = ("status", "event_type")
    search_fields = ("event_id", "event_type", "topic")
    readonly_fields = (
        "id",
        "event_id",
        "event_type",
        "topic",
        "envelope",
        "target_transports",
        "delivered_transports",
        "status",
        "attempts",
        "available_at",
        "locked_at",
        "published_at",
        "last_error_code",
        "created_at",
        "updated_at",
    )
    ordering = ("-created_at",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)


@admin.register(InboxEvent)
class InboxEventAdmin(admin.ModelAdmin):
    list_display = ("consumer_group", "event_id", "status", "lease_expires_at", "expires_at")
    list_filter = ("status", "consumer_group")
    search_fields = ("event_id", "consumer_group")
    readonly_fields = (
        "id",
        "consumer_group",
        "event_id",
        "status",
        "lease_expires_at",
        "expires_at",
        "created_at",
        "updated_at",
    )
    ordering = ("-updated_at",)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)
