"""Run bounded index downloads without holding up interpreter exit on Ctrl-C."""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future
from queue import Empty, SimpleQueue
from threading import Event, Thread
from typing import ParamSpec, TypeVar

_P = ParamSpec("_P")
_T = TypeVar("_T")


class DownloadPool:
    """Workers are daemon threads, with no executor exit hook to join them."""

    def __init__(self, workers: int) -> None:
        if workers <= 0:
            raise ValueError("workers must be greater than 0")
        self._limit = workers
        self._threads: list[Thread] = []
        self._queue: SimpleQueue[Callable[[bool], None] | None] = SimpleQueue()
        self._stop = Event()

    def _worker(self) -> None:
        while (job := self._queue.get()) is not None:
            job(self._stop.is_set())

    def submit(self, fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> Future[_T]:
        future: Future[_T] = Future()

        def run(cancel: bool) -> None:
            if cancel:
                future.cancel()
                return
            if future.set_running_or_notify_cancel():
                try:
                    result = fn(*args, **kwargs)
                except BaseException as exc:
                    future.set_exception(exc)
                else:
                    future.set_result(result)

        self._queue.put(run)
        if len(self._threads) < self._limit:
            thread = Thread(target=self._worker, daemon=True)
            thread.start()
            self._threads.append(thread)
        return future

    def close(self, *, wait: bool = True) -> None:
        if not wait:
            self._stop.set()
            while True:
                try:
                    job = self._queue.get_nowait()
                except Empty:
                    break
                if job is not None:
                    job(True)
        for _ in self._threads:
            self._queue.put(None)
        if wait:
            for thread in self._threads:
                thread.join()
