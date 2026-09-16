from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Coroutine
from typing import Any


class AsyncRuntime:
    """Own one event loop in one thread for sync and async compatibility APIs."""

    def __init__(self, *, name: str = "communication-runtime") -> None:
        self.name = name
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._closed = False
        self._lock = threading.RLock()

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("communication runtime is closed")
            if self._thread and self._thread.is_alive():
                return
            self._ready.clear()

            def target() -> None:
                loop = asyncio.new_event_loop()
                self._loop = loop
                asyncio.set_event_loop(loop)
                self._ready.set()
                try:
                    loop.run_forever()
                finally:
                    pending = asyncio.all_tasks(loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    loop.run_until_complete(loop.shutdown_asyncgens())
                    loop.close()
                    self._loop = None

            self._thread = threading.Thread(target=target, name=self.name, daemon=True)
            self._thread.start()
            if not self._ready.wait(timeout=10.0) or self._loop is None:
                raise RuntimeError("communication runtime failed to start")

    def submit(self, coroutine: Coroutine[Any, Any, Any]) -> concurrent.futures.Future[Any]:
        self.start()
        loop = self._loop
        if loop is None or not loop.is_running():
            coroutine.close()
            raise RuntimeError("communication runtime is unavailable")
        return asyncio.run_coroutine_threadsafe(coroutine, loop)

    def run(self, coroutine: Coroutine[Any, Any, Any], *, timeout: float | None = None) -> Any:
        return self.submit(coroutine).result(timeout=timeout)

    async def run_async(self, coroutine: Coroutine[Any, Any, Any], *, timeout: float | None = None) -> Any:
        future = self.submit(coroutine)
        wrapped = asyncio.wrap_future(future)
        return await asyncio.wait_for(wrapped, timeout=timeout) if timeout is not None else await wrapped

    def stop(self, *, timeout: float = 10.0) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            loop = self._loop
            thread = self._thread
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=timeout)
            self._thread = None
