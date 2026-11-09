from __future__ import annotations
from typing import Any, Optional
import abc
import math
import os
import threading
import time

from my import AbstractMethodException


def file_signature(stat: os.stat_result) -> tuple[int, int, int]:
    return stat.st_ino, stat.st_size, stat.st_mtime_ns


class FileWatcher(abc.ABC):
    """Tells Cronus when to re-read the crontab. A writer may be in the middle of a save (between the truncate and
    the write, or between the rename and the new file): a watcher reports a change only once the file looks saved"""

    def __init__(self, filename: str, settle_interval_seconds: float) -> None:
        self.__filename: str = filename
        self.__settle_interval_seconds: float = settle_interval_seconds  # how long a saved file stays the same

    def __enter__(self) -> FileWatcher:
        return self

    def __exit__(self, *_: object) -> None:
        pass

    @abc.abstractmethod
    def sleep_until_changed(self, seconds: float) -> bool:
        """Sleeps for `seconds`, or less: True once the file changed and looks saved.
        The change may be Cronus's own write"""
        raise AbstractMethodException()

    @abc.abstractmethod
    def reread_later(self) -> None:
        """The next sleep reports a change once the file looks saved, even with no new change:
        f.e. Cronus found the file changed or missing, and could not read it at once"""
        raise AbstractMethodException()

    def is_saved(self) -> bool:
        """The file exists and stays the same for the settle interval: no writer is in the middle of a save.
        Sleeps that long"""
        try:
            before: Optional[tuple[int, int, int]] = file_signature(os.stat(self.__filename))
        except FileNotFoundError:
            before = None
        time.sleep(self.__settle_interval_seconds)
        try:
            return before == file_signature(os.stat(self.__filename))
        except FileNotFoundError:
            return False


class PollingWatcher(FileWatcher):
    """Runs `os.stat` every check interval. Needs nothing from outside"""

    def __init__(self, filename: str, check_interval_seconds: float = 1) -> None:
        super().__init__(filename, check_interval_seconds)  # a change is read once it stays the same for one check
        self.__filename: str = filename
        self.__check_interval_seconds: float = check_interval_seconds  # real time
        self.__checked_at: float = -math.inf  # time.monotonic() of the last check
        self.__read_signature: Optional[tuple[int, int, int]] = None  # of the last reported change
        self.__unstable_signature: Optional[tuple[int, int, int]] = None  # seen once, not reported yet
        self.__read_at: float = -math.inf  # time.time() of the last reported change

    def sleep_until_changed(self, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while True:
            next_check_at = self.__checked_at + self.__check_interval_seconds
            now = time.monotonic()
            if now >= next_check_at:
                if self.__check():
                    return True
            elif now >= deadline:
                return False
            else:
                time.sleep(min(deadline, next_check_at) - now)

    def reread_later(self) -> None:
        self.__read_signature = None

    def __check(self) -> bool:
        self.__checked_at = time.monotonic()
        try:
            stat = os.stat(self.__filename)
            signature = file_signature(stat)
            if signature != self.__read_signature:
                # a writer may be between the truncate and the write:
                # report only once the file has stayed the same for one check
                if signature != self.__unstable_signature:
                    self.__unstable_signature = signature
                    return False
            # coarse mtime (f.e. 1s on HFS+) may hide a later write within the same tick:
            # report again until the last report is clearly after that tick.
            # Real time, not Clock: compared with the filesystem's time
            elif self.__read_at - stat.st_mtime >= 2:
                return False
            self.__read_signature = signature
            self.__read_at = time.time()
            return True
        except FileNotFoundError:  # an editor may save by rename: the file is back by a later check
            self.__unstable_signature = None
            return False


class EventWatcher(FileWatcher):
    """Wakes up on the file system's events (inotify on Linux, FSEvents on macOS) through the `watchdog` package
    from pip, which only this watcher imports. It watches the directory: a save by rename replaces the file.
    It reports a change once no event came for the settle interval: a writer may be between the truncate and the write"""

    def __init__(self, filename: str, settle_interval_seconds: float = 0.5) -> None:
        super().__init__(filename, settle_interval_seconds)
        self.__path: str = os.path.realpath(filename)  # events come with real paths
        self.__settle_interval_seconds: float = settle_interval_seconds
        self.__changed_at: Optional[float] = None  # time.monotonic() of the last event, not reported yet
        self.__changed_at_lock: threading.Lock = threading.Lock()  # the observer's thread sets it
        self.__woken: threading.Event = threading.Event()  # set on each event, to end a sleep
        self.__observer: Optional[Any] = None

    def __enter__(self) -> EventWatcher:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler  # pip install watchdog
        from watchdog.observers import Observer
        on_event = self.__on_event

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event: FileSystemEvent) -> None:
                on_event(event)

        observer = Observer()
        observer.schedule(Handler(), os.path.dirname(self.__path))
        observer.start()
        self.__observer = observer
        return self

    def __exit__(self, *_: object) -> None:
        if observer := self.__observer:
            observer.stop()
            observer.join()
            self.__observer = None

    def sleep_until_changed(self, seconds: float) -> bool:
        deadline = time.monotonic() + seconds
        while True:
            self.__woken.clear()
            now = time.monotonic()
            wake_at = deadline
            with self.__changed_at_lock:
                if (changed_at := self.__changed_at) is not None:
                    settled_at = changed_at + self.__settle_interval_seconds
                    if now >= settled_at:
                        self.__changed_at = None
                        return True
                    wake_at = min(deadline, settled_at)
            if now >= deadline:
                return False  # a change still settling is reported by a later sleep
            self.__woken.wait(wake_at - now)

    def reread_later(self) -> None:
        self.__mark_changed()

    def __on_event(self, event: Any) -> None:
        paths = (event.src_path, event.dest_path)  # dest_path: of a move, f.e. a save by rename; else '' (watchdog 4+)
        if self.__path in (os.path.realpath(os.fsdecode(path)) for path in paths if path):
            self.__mark_changed()

    def __mark_changed(self) -> None:
        with self.__changed_at_lock:
            self.__changed_at = time.monotonic()
        self.__woken.set()
