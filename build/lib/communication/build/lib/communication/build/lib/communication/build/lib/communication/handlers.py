from __future__ import annotations

import asyncio
import fnmatch
import inspect
import logging
from dataclasses import dataclass, field
from typing import Callable, Mapping

from .envelope import EventEnvelope

logger = logging.getLogger(__name__)
EventHandler = Callable[[EventEnvelope, "EventContext"], object]


@dataclass(frozen=True, slots=True)
class EventContext:
    transport: str
    topic: str
    headers: Mapping[str, str]
    key: str | None = None
    partition: int | None = None
    offset: int | None = None
    sequence: int | None = None
    delivery_count: int = 1
    metadata: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RegisteredHandler:
    pattern: str
    callback: EventHandler
    name: str
    priority: int = 0


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: list[RegisteredHandler] = []

    def register(
        self,
        pattern: str,
        callback: EventHandler | None = None,
        *,
        name: str | None = None,
        priority: int = 0,
    ):
        def decorator(fn: EventHandler) -> EventHandler:
            if not callable(fn):
                raise TypeError("event handler must be callable")
            safe_pattern = str(pattern).strip()
            if not safe_pattern or len(safe_pattern) > 255:
                raise ValueError("event handler pattern is invalid")
            handler_name = name or f"{fn.__module__}.{getattr(fn, '__qualname__', fn.__name__)}"
            if any(item.name == handler_name and item.pattern == safe_pattern for item in self._handlers):
                raise ValueError(f"handler {handler_name!r} is already registered for {safe_pattern!r}")
            self._handlers.append(
                RegisteredHandler(
                    pattern=safe_pattern,
                    callback=fn,
                    name=handler_name,
                    priority=int(priority),
                )
            )
            self._handlers.sort(key=lambda item: (-item.priority, item.name))
            return fn

        return decorator(callback) if callback is not None else decorator

    def unregister(self, name: str) -> int:
        before = len(self._handlers)
        self._handlers = [item for item in self._handlers if item.name != name]
        return before - len(self._handlers)

    def matching(self, event_type: str) -> tuple[RegisteredHandler, ...]:
        return tuple(item for item in self._handlers if fnmatch.fnmatchcase(event_type, item.pattern))

    async def dispatch(self, event: EventEnvelope, context: EventContext) -> None:
        handlers = self.matching(event.event_type)
        if not handlers:
            logger.debug("no event handler registered", extra={"event_type": event.event_type})
            return
        for item in handlers:
            callback = item.callback
            if inspect.iscoroutinefunction(callback):
                result = await callback(event, context)
            else:
                result = await asyncio.to_thread(callback, event, context)
                if inspect.isawaitable(result):
                    result = await result
            if result is False:
                raise RuntimeError(f"event handler {item.name} rejected the event")


_default_registry = HandlerRegistry()


def event_handler(pattern: str, *, name: str | None = None, priority: int = 0):
    return _default_registry.register(pattern, name=name, priority=priority)


def get_default_handler_registry() -> HandlerRegistry:
    return _default_registry
