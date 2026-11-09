from __future__ import annotations
from datetime import datetime, timedelta
from types import FrameType
from typing import Callable, Union, Iterable, Optional, TypeVar
import abc
import argparse
import calendar
import contextlib
import fcntl
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import file_watcher
import instance_lock

from my import AbstractMethodException

# todo: allow manual decrease of last_call
# todo: allow time without seconds
# todo: check tasks to be unique?
# todo: save checkpoint if task was just executed, and next one is later then (next checkpoint)?
# todo: support for concrete dates
# todo: end time for task (0 0 0 - 5 0 0 echo "sleep")
# todo: do not execute task, if updated, and time passed (* 0 45 -> * 1 0, now = 1:15)
# todo: why huge CPU load for XLS file, that is already open?
# todo: test daylight saving time
# todo: sleep 0.02 in test
# external
# todo: remove time workaround when freezegun is fixed
# todo: push unittest-data-provider
# todo: linux app/package


sep = '[ \\t]'
nr1 = '(?:\\*|\\*/\\d+|\\d+|\\d+-\\d+)'
nr = '(' + nr1 + '(?:,' + nr1 + ')*' + ')'
command = '(?:\'[^\']*\'|"[^"]*"|[^#\'"]+)+'
time_value = '(\\d+|\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})'
status = ' #' + time_value + '(?: \\| ' + time_value + ')?'  # called at | alive at
end = '\\n?$'
# comment = '#(?:(?!' + status + end + ').)*'
comment = '#.*'
beginning = '^' + sep + '*'
pattern = beginning + (nr + sep + '+') * 6 + '(' + command + ')' + '(' + comment + ')?' + end


def alert(error: str | BaseException) -> None:
    # print(traceback.format_exc())
    error = str(error)
    _, _, _traceback = sys.exc_info()
    if _traceback:
        filename = os.path.split(_traceback.tb_frame.f_code.co_filename)[1]
        error += f" at {filename}:{_traceback.tb_lineno}"
    subprocess.call(['alert', f"{Cronus.__name__}: {error}"])


T = TypeVar('T')
def must(a: Optional[T]) -> T:
    assert a is not None
    return a


stop_poll_interval_seconds = 0.05  # how often stop_groups checks whether the groups are gone
watchdog_option = '--watchdog'


def stop_groups(pgids: Iterable[int],
                timeout: timedelta,
                clear_zombies: Callable[[], object] = lambda: None) -> None:
    """SIGTERM to each process group, then SIGKILL to the groups still alive after the timeout.
    A process that left its group (setsid, f.e.) is not reached.
    A finished child stays a zombie until its parent collects its exit status, and a zombie keeps its group alive:
    `clear_zombies` collects them for the processes that are the caller's children"""
    pgids = signal_groups(pgids, signal.SIGTERM)
    deadline = time.monotonic() + timeout.total_seconds()
    while pgids and time.monotonic() < deadline:
        time.sleep(stop_poll_interval_seconds)
        clear_zombies()
        pgids = signal_groups(pgids, 0)
    signal_groups(pgids, signal.SIGKILL)  # the caller collects the killed children: they are not dead yet


def signal_groups(pgids: Iterable[int], sig: int) -> list[int]:
    """Returns the groups that still exist"""
    alive = []
    for pgid in pgids:
        try:
            os.killpg(pgid, sig)
            alive.append(pgid)
        except ProcessLookupError:
            pass
        except PermissionError as error:  # f.e. a setuid member: the group is still there
            alive.append(pgid)
            # not later: on macOS, a group of zombies only also refuses, until their parent collects them
            if sig == signal.SIGTERM:
                alert(error)
    return alive


class Watchdog:
    """Stops the tasks' process groups if Cronus dies without cleanup (f.e. by SIGKILL).
    It is a separate process: Cronus sends it `+<pgid>` and `-<pgid>` lines through a pipe.
    When Cronus exits in any way, the OS closes the pipe, and the watchdog stops the groups left"""

    def __init__(self, stop_timeout: timedelta) -> None:
        self.__stop_timeout: timedelta = stop_timeout
        self.__exit_margin: timedelta = timedelta(seconds=1)  # for the process to exit after it stopped the groups
        self.__process: Optional[subprocess.Popen] = None
        self.__pgids: set[int] = set()  # the groups the process knows about

    def __enter__(self) -> Watchdog:
        # own session: Ctrl+C in a terminal reaches only Cronus, which stops the tasks itself
        self.__process = subprocess.Popen((sys.executable,
                                           os.path.abspath(__file__),
                                           watchdog_option,
                                           str(self.__stop_timeout.total_seconds())),
                                          stdin=subprocess.PIPE,
                                          text=True,
                                          start_new_session=True)
        return self

    def __exit__(self, *_: object) -> None:
        if process := self.__process:
            self.__process = None
            try:
                must(process.stdin).close()  # EOF: the process stops the groups still registered and exits
                process.wait((self.__stop_timeout + self.__exit_margin).total_seconds())
            except (OSError, subprocess.TimeoutExpired) as error:
                alert(error)

    def sync(self, pgids: set[int]) -> None:
        """Registers the new groups, and unregisters the ended ones"""
        lines = ([f'+{pgid}\n' for pgid in pgids - self.__pgids]
                 + [f'-{pgid}\n' for pgid in self.__pgids - pgids])
        self.__pgids = pgids
        if lines and (process := self.__process):
            try:
                stdin = must(process.stdin)
                stdin.writelines(lines)
                stdin.flush()
            except OSError as error:  # the process is gone: alert once, go on without it
                self.__process = None
                alert(error)

    @staticmethod
    def serve(stop_timeout: timedelta) -> None:
        """The watchdog process itself"""
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, signal.SIG_IGN)  # f.e. `pkill -f cronus`: stop only on EOF, after Cronus
        pgids: set[int] = set()
        for line in sys.stdin:
            try:
                pgid = int(line[1:])
            except ValueError:  # f.e. a partial line, if Cronus was interrupted mid-write: still stop the rest on EOF
                continue
            if line[0] == '+':
                pgids.add(pgid)
            else:
                pgids.discard(pgid)
        stop_groups(pgids, stop_timeout)


class Clock:
    def time(self) -> datetime:
        return datetime.now()


status_fmt_timestamp = 1
status_fmt_datetime = 2


class Status:
    def __init__(self,
                 _format: int,
                 called_at: datetime,
                 alive_at: Optional[datetime] = None) -> None:
        self.format: int = _format
        self.called_at: datetime = called_at
        self.alive_at: Optional[datetime] = alive_at  # last time the process started at `called_at` was seen running

    @staticmethod
    def from_string(string: str) -> Status | None:
        if _status := re.search("^.+" + status + end, string):
            _called_at, _alive_at = _status.groups()
            return Status(status_fmt_timestamp if _called_at.isdigit() else status_fmt_datetime,
                          Status.__parse(_called_at),
                          Status.__parse(_alive_at) if _alive_at else None)
        return None

    def __str__(self) -> str:
        string = self.__format(self.called_at)
        if self.alive_at:
            string += ' | ' + self.__format(self.alive_at)
        return string

    def __format(self, _datetime: datetime) -> str:
        if self.format == status_fmt_timestamp:
            return str(int(_datetime.timestamp()))
        elif self.format == status_fmt_datetime:
            return _datetime.strftime("%Y-%m-%d %H:%M:%S")
        else:
            raise Exception(f"Unknown format: {self.format}")

    @staticmethod
    def __parse(string: str) -> datetime:
        if string.isdigit():
            return datetime.fromtimestamp(int(string))
        return datetime.strptime(string, "%Y-%m-%d %H:%M:%S")

    def merge(self, saved: Status) -> None:
        """The newest of each time wins: a stale save must not take a time back.
        So the line is rewritten only if some of its saved times is older"""
        self.format = saved.format
        self.called_at = max(self.called_at, saved.called_at)
        # an older alive_at belongs to an older call
        alive_at = max(filter(None, (self.alive_at, saved.alive_at)), default=None)
        self.alive_at = alive_at if alive_at and alive_at >= self.called_at else None


class Task:
    __process: Union[subprocess.Popen, None] = None

    def __init__(self,
                 original_string: str,
                 months: str,
                 days: str,
                 weekdays: str,
                 hours: str,
                 minutes: str,
                 seconds: str,
                 _command: str,
                 _status: Status) -> None:
        self.__original_string: str = original_string
        self.__months: list[int] = Task.__values(months, 1, 12)
        self.__days: list[int] = Task.__values(days, 1, 31)
        self.__weekdays: list[int] = Task.__values(weekdays, 1, 7)
        if 7 in self.__weekdays:
            self.__weekdays.remove(7)
            self.__weekdays.insert(0, 0)
        self.__hours: list[int] = Task.__values(hours, 0, 23)
        self.__minutes: list[int] = Task.__values(minutes, 0, 59)
        self.__seconds: list[int] = Task.__values(seconds, 0, 59)
        self.__command: str = _command
        self.__status: Status = _status
        self.__expected_last_call(datetime(3000, 1, 1))  # todo: check if it may ever be called

    def __del__(self) -> None:
        # Python is shutting down: what stopping needs may be gone already. Cronus has stopped its tasks before,
        # and if it died without cleanup, the watchdog stops them
        if sys.is_finalizing():
            return
        Task.stop([self], timedelta(seconds=5))

    @staticmethod
    def stop(tasks: Iterable[Task], timeout: timedelta) -> None:
        """Stops the processes of all tasks at once, so it takes at most the timeout however many tasks there are."""
        processes: list[subprocess.Popen] = []
        for task in tasks:
            if process := task.__get_running_process():
                processes.append(process)
        if processes:
            stop_groups([process.pid for process in processes],
                        timeout,
                        lambda: [_process.poll() for _process in processes])  # poll() collects the exit status
            for process in processes:
                # after SIGKILL, a shell is not dead at once. Until it is collected, its zombie keeps the group
                # alive, the task counts as running, and the watchdog would wait for it until its own timeout
                try:
                    process.wait(1)
                except subprocess.TimeoutExpired as error:  # f.e. it changed its real uid (sudo): SIGKILL not permitted
                    alert(error)

    def process_group(self) -> Optional[int]:
        """The task's shell leads its own process group, which holds all processes it started"""
        return process.pid if (process := self.__get_running_process()) else None

    @staticmethod
    def from_string(string: str, now: datetime) -> Union[Task, None]:
        global pattern, beginning, comment, status, end

        if re.match(beginning + comment, string) or re.match(beginning + end, string):
            return None
        task = re.search(pattern, string)
        if not task:
            raise Exception('Wrong format for task: ' + string)
        groups = task.groups()
        status_idx = 7
        if len(groups) != status_idx + 1:
            raise Exception('Task parsed incorrectly')
        _status = Status.from_string(string) or Status(status_fmt_datetime, now)
        string = must(re.match('^((?:(?!' + status + ').)*)(?:' + status + ')?' + end, string)).group(1)
        # pyrefly: ignore[bad-argument-type]
        return Task(*([string] + [group.strip() for group in list(groups[:-1])] + [_status]))

    def __str__(self) -> str:
        return self.__original_string + ' #' + self.__status.__str__()

    def is_skipped(self, now: datetime) -> bool:
        __last_call = self.__status.alive_at or self.__status.called_at
        return __last_call < self.__expected_last_call(now)

    def calls(self, _from: datetime, _to: datetime) -> list[datetime]:
        _calls = []
        _from = max(_from, self.__status.called_at + timedelta(microseconds=1)) if self.__status else _from
        year = Task.__year_start(_from)
        while True:
            for month in self.__months:  # todo: refactor
                __time = year.replace(month=month)
                if Task.__add_month(__time) >= _from:
                    for day in self.__days:
                        if Task.__is_correct_date(__time, day=day):
                            __time = __time.replace(day=day)
                            if __time + timedelta(days=1) >= _from:
                                for weekday in self.__weekdays:
                                    if Task.__is_correct_date(__time, weekday=weekday):
                                        for hour in self.__hours:
                                            __time = __time.replace(hour=hour)
                                            if __time + timedelta(hours=1) >= _from:
                                                for minute in self.__minutes:
                                                    __time = __time.replace(minute=minute)
                                                    if __time + timedelta(minutes=1) >= _from:
                                                        for second in self.__seconds:
                                                            __time = __time.replace(second=second)
                                                            if __time >= _to:
                                                                return _calls
                                                            if __time >= _from:
                                                                _calls.append(__time)
            year = year.replace(year=year.year + 1)
            if year > _to:
                return _calls

    def execute(self, now: datetime) -> bool:
        if self.__get_running_process():
            self.__status.alive_at = now
            return False
        self.__run()
        self.__set_called_at(now)
        self.__status.alive_at = None
        return True

    def equals(self, other: Task) -> bool:  # todo: cover with test
        return self.__original_string == other.__original_string

    def merge_status(self, other: Task) -> None:
        self.__status.merge(other.__status)

    def line(self) -> str:
        return self.__original_string

    def status(self) -> Status:
        return self.__status

    def set_status(self, _status: Status) -> None:
        self.__status = _status

    @staticmethod
    def __values(value: str, _min: int, _max: int) -> list[int]:
        values = sorted(list(set(Task.__calc_values(value, _min, _max))))
        for v in values:
            if not _min <= v <= _max:
                raise Exception
        return values

    @staticmethod
    def __calc_values(value: str, _min: int, _max: int) -> list[int]:
        values = []
        for value in value.split(','):
            if value.isdigit():
                values.append(int(value))
            else:
                _range = range(_min, _max + 1)
                if value == '*':
                    values += list(_range)
                elif value[:2] == '*/':
                    values += [v for v in _range if v % int(value[2:]) == 0]
                elif match := re.search('(\\d+)-(\\d+)', value):
                    ints: list[int] = [int(v) for v in match.groups()]
                    if ints[1] <= ints[0]:
                        raise Exception('Incorrect bounds')
                    values += list(range(ints[0], ints[1] + 1))
                else:
                    raise Exception('Unknown format')
        return values

    def __expected_last_call(self, now: datetime) -> datetime:
        years = 0
        year = Task.__year_start(now)
        while True:
            for month in reversed(self.__months):  # todo: refactor
                __time = year.replace(month=month)
                if __time <= now:
                    for day in reversed(self.__days):
                        if Task.__is_correct_date(__time, day=day):
                            __time = __time.replace(day=day)
                            if __time <= now:
                                for weekday in reversed(self.__weekdays):
                                    if Task.__is_correct_date(__time, weekday=weekday):
                                        for hour in reversed(self.__hours):
                                            __time = __time.replace(hour=hour)
                                            if __time <= now:
                                                for minute in reversed(self.__minutes):
                                                    __time = __time.replace(minute=minute)
                                                    if __time <= now:
                                                        for second in reversed(self.__seconds):
                                                            __time = __time.replace(second=second)
                                                            if now > __time:
                                                                return __time
            year = year.replace(year=year.year - 1)
            years += 1
            if years > 28:
                raise Exception('Task is never to be executed: ' + self.__original_string)

    def __run(self) -> None:
        try:
            self.__process = subprocess.Popen(
                self.__command,
                shell=True,
                start_new_session=True,  # own process group: stopping the task reaches all it started, even orphaned grandchildren
            )
        except Exception as exception:
            alert(exception)

    def __set_called_at(self, _datetime: datetime) -> None:
        self.__status.called_at = _datetime

    def __get_running_process(self) -> Union[subprocess.Popen, None]:
        if self.__process and self.__process.poll() is not None:
            self.__process = None
        return self.__process

    @staticmethod
    def __year_start(base: datetime) -> datetime:
        return base.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def __add_month(base: datetime) -> datetime:
        month = base.month
        year = base.year
        month += 1
        if month == 13:
            year += 1
            month = 1
        return base.replace(year=year, month=month)

    @staticmethod
    def __is_correct_date(base: datetime,
                          day: Optional[int] = None,
                          weekday: Optional[int] = None) -> bool:
        if day is not None:
            if day > calendar.monthrange(base.year, base.month)[1]:
                return False
            base = base.replace(day=day)
        if weekday is not None and weekday != int(base.strftime('%w')):
            return False
        return True


class FileChangedException(Exception):
    pass


class WakeUpException(Exception):
    pass


class Event:
    def __init__(self, _datetime: datetime, tasks: list[Task]) -> None:
        self.datetime: datetime = _datetime
        self.tasks: list[Task] = tasks


def write_atomically(filename: str, content: str, may_replace: Callable[[], bool] = lambda: True) -> bool:
    """Nobody (user's editor, f.e.) ever sees a partially written file.
    Writes nothing, and returns False, if `may_replace()` says no: it is checked after the slow sync,
    right before the replace, so that the file can hardly change in between"""
    path = os.path.realpath(filename)  # replace the target, keep the symlink
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), prefix=os.path.basename(path) + '.')
    replaced = False
    try:
        with os.fdopen(fd, 'w') as file:
            file.write(content)
            file.flush()
            # the data is on disk before the rename, so a power loss leaves the old or the new file, never empty
            if hasattr(fcntl, 'F_FULLFSYNC'):  # macOS: fsync does not flush the drive's cache
                fcntl.fcntl(file, fcntl.F_FULLFSYNC)
            else:
                os.fsync(file)
        if not may_replace():
            return False
        try:
            shutil.copymode(path, tmp_path)
            stat = os.stat(path)
            tmp_stat = os.stat(tmp_path)  # not the egid: f.e. on macOS, a new file takes its directory's group
            if (stat.st_uid, stat.st_gid) != (tmp_stat.st_uid, tmp_stat.st_gid):
                try:
                    os.chown(tmp_path, stat.st_uid, stat.st_gid)  # keep the owner, f.e. when run by root
                except PermissionError:
                    pass  # only root may give a file away: the file becomes ours
        except FileNotFoundError:
            pass  # a new file
        os.replace(tmp_path, path)
        replaced = True
        return True
    finally:
        if not replaced:
            os.unlink(tmp_path)


def read_lines(filename: str) -> Optional[list[str]]:
    """None if the file is missing"""
    try:
        with open(filename, 'r') as file:
            return file.readlines()
    except FileNotFoundError:
        return None


class StatusStore(abc.ABC):
    """Where the tasks' statuses (the last calls) are kept between runs of Cronus"""

    @abc.abstractmethod
    def merge(self, task: Task, read: Task) -> None:
        """On re-read of the crontab: `task` is kept, `read` is its line as read now"""
        raise AbstractMethodException()

    @abc.abstractmethod
    def restore(self, tasks: list[Task]) -> None:
        """On re-read of the crontab: gives new tasks their saved statuses"""
        raise AbstractMethodException()

    @abc.abstractmethod
    def save(self, tasks: dict[int, Task], lines: list[str]) -> Optional[list[str]]:
        """`tasks` by their line index in `lines`, the crontab as last read.
        Returns the crontab's lines after the save, or None if not saved, and to be tried again later"""
        raise AbstractMethodException()


class CrontabStore(StatusStore):
    """In the crontab itself: each task line ends with ` #<called_at> | <alive_at>`. All is in one file,
    but Cronus writes the file that the user edits: it never overwrites the user's changes, and merges stale saves"""

    def __init__(self, crontab: str) -> None:
        self.__crontab: str = crontab

    def merge(self, task: Task, read: Task) -> None:
        task.merge_status(read)  # the newer times are kept: a stale save does not take a time back

    def restore(self, tasks: list[Task]) -> None:
        pass  # a new line holds its status itself

    def save(self, tasks: dict[int, Task], lines: list[str]) -> Optional[list[str]]:
        """None if the user has changed the crontab, or it is missing (f.e. a save by rename):
        their changes are re-read first, once the file is stable"""
        new_lines = lines[:]
        for task_id, task in tasks.items():
            new_lines[task_id] = task.__str__() + '\n'
        if new_lines == lines:
            return lines
        if not write_atomically(self.__crontab, ''.join(new_lines), lambda: read_lines(self.__crontab) == lines):
            return None
        return new_lines


StateEntry = tuple[str, float, Optional[float]]  # line, called_at, alive_at


class StateFileStore(StatusStore):
    """In a separate JSON file, by task line: Cronus never writes the crontab.
    A line's status suffix, if any, is only used until the state file has the line"""

    def __init__(self, crontab: str) -> None:
        self.__filename: str = crontab + '.state'
        self.__saved: Optional[str] = None  # the content last written

    def merge(self, task: Task, read: Task) -> None:
        pass  # the status in memory is the newest: the crontab is not written, so its suffixes are stale

    def restore(self, tasks: list[Task]) -> None:
        saved = self.__load()
        for task in tasks:
            for i, (line, called_at, alive_at) in enumerate(saved):
                if line == task.line():  # identical lines take the entries in order
                    task.set_status(Status(status_fmt_datetime,
                                           datetime.fromtimestamp(called_at),
                                           datetime.fromtimestamp(alive_at) if alive_at else None))
                    del saved[i]
                    break

    def save(self, tasks: dict[int, Task], lines: list[str]) -> Optional[list[str]]:
        entries = []
        for task in tasks.values():
            _status = task.status()
            entries.append([task.line(),
                            _status.called_at.timestamp(),
                            _status.alive_at.timestamp() if _status.alive_at else None])
        content = json.dumps(entries, indent=1) + '\n'
        if content != self.__saved:
            write_atomically(self.__filename, content)
            self.__saved = content
        return lines  # the crontab is not written

    def __load(self) -> list[StateEntry]:
        try:
            with open(self.__filename, 'r') as file:
                entries = json.load(file)
            if not isinstance(entries, list) or not all(map(StateFileStore.__is_entry, entries)):
                raise ValueError("expected a list of [line, called_at, alive_at]")
            return [(line, called_at, alive_at) for line, called_at, alive_at in entries]
        except FileNotFoundError:
            return []
        except ValueError as error:  # f.e. edited by hand: the statuses in the lines, or "now", are used
            alert(f"{self.__filename}: {error}")
            return []

    @staticmethod
    def __is_entry(entry: object) -> bool:
        return (isinstance(entry, list)
                and len(entry) == 3
                and isinstance(entry[0], str)
                and isinstance(entry[1], (int, float))
                and (entry[2] is None or isinstance(entry[2], (int, float))))


class Crontab:
    """The crontab file: all access to it, and what is bound to it: its watcher, its status store and its instance
    lock. Each is made from the file's path here, so none of them can be given another file"""

    def __init__(self,
                 filename: str,
                 watcher: Callable[[str], file_watcher.FileWatcher],
                 store: Callable[[str], StatusStore],
                 lock: Callable[[str], instance_lock.InstanceLock]) -> None:
        self.__filename: str = filename
        self.__watcher: file_watcher.FileWatcher = watcher(filename)
        self.__store: StatusStore = store(filename)
        self.__lock: instance_lock.InstanceLock = lock(filename)
        self.__settle_timeout: timedelta = timedelta(seconds=5)  # how long it may keep changing or be missing before an alert
        self.__exit_stack: Optional[contextlib.ExitStack] = None

    def __str__(self) -> str:
        return self.__filename

    def __enter__(self) -> Crontab:
        """The lock first: a second instance must not even start watching"""
        with contextlib.ExitStack() as stack:  # if the watcher fails to start, the lock is released at once
            stack.enter_context(self.__lock)
            stack.enter_context(self.__watcher)
            self.__exit_stack = stack.pop_all()
        return self

    def __exit__(self, *exc_info: object) -> None:
        if exit_stack := self.__exit_stack:
            self.__exit_stack = None
            exit_stack.close()  # the watcher first, then the lock

    def read(self) -> list[str]:
        """Raises FileNotFoundError if missing, f.e. in the middle of a save by rename"""
        with open(self.__filename, 'r') as file:
            return file.readlines()

    def is_changed_from(self, lines: list[str]) -> bool:
        """False if missing: an editor may save by rename, and the watcher reports the file once it is back"""
        current = read_lines(self.__filename)
        if current is None:
            self.__watcher.reread_later()  # even if it comes back unchanged, f.e. renamed back
            return False
        return current != lines

    def sleep_until_changed(self, seconds: float) -> bool:
        """See `FileWatcher.sleep_until_changed`: the change may be Cronus's own write"""
        return self.__watcher.sleep_until_changed(seconds)

    def is_saved(self) -> bool:
        """See `FileWatcher.is_saved`"""
        return self.__watcher.is_saved()

    def wait_until_saved(self) -> None:
        """Waits as long as it takes: Cronus cannot run without the file. Alerts once, if it takes too long"""
        deadline = time.monotonic() + self.__settle_timeout.total_seconds()
        alerted = False
        while not self.__watcher.is_saved():
            if not alerted and time.monotonic() > deadline:
                alert(f"{self.__filename} keeps changing or is missing")
                alerted = True

    def merge(self, task: Task, read: Task) -> None:
        """See `StatusStore.merge`"""
        self.__store.merge(task, read)

    def restore(self, tasks: list[Task]) -> None:
        """See `StatusStore.restore`"""
        self.__store.restore(tasks)

    def save(self, tasks: dict[int, Task], lines: list[str]) -> Optional[list[str]]:
        """See `StatusStore.save`. If not saved, the store found the file changed by the user, or missing:
        then the watcher reports it once it looks saved. Re-reading it at once might catch it half-written"""
        saved = self.__store.save(tasks, lines)
        if saved is None:
            self.__watcher.reread_later()
        return saved


class Tasks:
    """The crontab's tasks by line index, kept in sync with its lines, and their processes.
    They are stopped on exit; the watchdog stops them if Cronus dies without cleanup"""

    def __init__(self, crontab: Crontab) -> None:
        self.__crontab: Crontab = crontab
        self.__stop_timeout: timedelta = timedelta(seconds=5)  # SIGTERM to the tasks' processes, then SIGKILL after this
        self.__tasks: dict[int, Task] = {}  # by line index
        self.__lines: list[str] = []  # the crontab as last read or saved
        self.__watchdog: Watchdog = Watchdog(self.__stop_timeout)

    def __enter__(self) -> Tasks:
        self.__watchdog.__enter__()
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Stops the processes of all tasks at once"""
        try:
            Task.stop(self.__tasks.values(), self.__stop_timeout)
            self.sync_watchdog()
        finally:
            self.__watchdog.__exit__(*exc_info)

    def by_line(self) -> dict[int, Task]:
        return dict(self.__tasks)

    def skipped(self, now: datetime) -> list[Task]:
        return [task for task in self.__tasks.values() if task.is_skipped(now)]

    def read(self, now: datetime) -> None:
        """Re-reads the crontab. An unchanged line keeps its task, with its process and its last call.
        The tasks of the lines gone are stopped. Raises FileNotFoundError if the crontab is missing.
        `now` is the last call of a new line without a status"""
        new_tasks: dict[int, Task] = {}
        lines = self.__crontab.read()  # not in self yet: if interrupted, a save must not mix new lines with old tasks
        for task_id in range(len(lines)):
            try:
                if task := Task.from_string(lines[task_id], now):
                    new_tasks[task_id] = task
            except Exception as exception:
                alert(exception)

        old_tasks = list(self.__tasks.values())
        updated_tasks: dict[int, Task] = {}
        added_tasks: list[Task] = []
        for new_task_id, new_task in new_tasks.items():
            for old_task in old_tasks:
                if new_task.equals(old_task):
                    old_tasks.remove(old_task)
                    self.__crontab.merge(old_task, new_task)
                    new_task = old_task
                    break
            else:
                added_tasks.append(new_task)
            updated_tasks[new_task_id] = new_task
        self.__crontab.restore(added_tasks)
        self.__lines = lines
        self.__tasks = updated_tasks
        Task.stop(old_tasks, self.__stop_timeout)
        self.sync_watchdog()

    def save(self) -> bool:
        """Returns False if not saved: see `Crontab.save`"""
        if self.__tasks and self.__lines:
            lines = self.__crontab.save(self.__tasks, self.__lines)
            if lines is None:
                return False
            self.__lines = lines  # compared on a change: Cronus's own write is not a user's change
        return True

    def is_crontab_changed(self) -> bool:
        """The crontab differs from the lines last read or saved: not just Cronus's own write"""
        return self.__crontab.is_changed_from(self.__lines)

    def execute(self, tasks: Iterable[Task], now: datetime) -> bool:
        """Returns whether any command was called: not for a task whose previous process is alive"""
        called = False
        for task in tasks:
            called = task.execute(now) or called
        self.sync_watchdog()
        return called

    def sync_watchdog(self) -> None:
        """Tells the watchdog the process groups of the running tasks"""
        pgids: set[int] = set()
        for task in self.__tasks.values():
            if pgid := task.process_group():
                pgids.add(pgid)
        self.__watchdog.sync(pgids)


class Cronus:
    def __init__(self,
                 crontab: Crontab,
                 clock: Clock,
                 sleep_interval_seconds: float = 5) -> None:
        self.__queue_interval: timedelta        = timedelta(days=1)
        self.__wakeup_interval_seconds: float   = timedelta(minutes=10).total_seconds()
        self.__saving_interval: timedelta       = timedelta(seconds=5)  # after a call
        # when only alive_at changed: a still running process. After a reboot, alive_at may be this old,
        # and a call dropped within that time (the process was alive) then runs as skipped
        self.__alive_saving_interval: timedelta = timedelta(minutes=1)
        self.__save_timeout: timedelta          = timedelta(seconds=5)  # how long the save on exit retries while the file is unsaved
        self.__sleep_interval_seconds: float    = sleep_interval_seconds  # the longest sleep before Cronus reads the clock again

        self.__crontab: Crontab = crontab
        self.__clock: Clock = clock  # workaround, because python's unittest cannot mock with lambda
        self.__tasks: Tasks = Tasks(crontab)
        self.__time: Optional[datetime] = None
        self.__checkpoint: Optional[datetime] = None
        self.__next_events: list[Event] = []
        self.__called_since_save: bool = False

    def run(self) -> None:
        with self.__crontab, self.__tasks:  # on exit, the tasks are stopped after the save
            try:
                self.__update_time()
                self.__checkpoint = self.__last_checkpoint()
                while True:
                    try:
                        try:
                            self.__read()
                        except FileNotFoundError:  # gone again since the check, f.e. a save by rename
                            self.__crontab.wait_until_saved()
                            continue
                        self.__update_time()
                        self.__write()  # at once: the saved times must be the actual ones, even after a stale save
                        self.__execute(self.__tasks.skipped(self.__clock.time()))
                        while True:
                            self.__execute(self.__next_tasks())
                    except FileChangedException:  # the watcher has already waited until the file looks saved
                        continue
                    except WakeUpException:
                        self.__crontab.wait_until_saved()  # an editor may be in the middle of a save
                        continue
            finally:  # also SystemExit (SIGTERM) and KeyboardInterrupt
                self.__save()

    def __save(self) -> None:
        """On exit, no watcher reports the user's changes: read them here, once the file looks saved"""
        deadline = time.monotonic() + self.__save_timeout.total_seconds()
        while not self.__write():
            if time.monotonic() > deadline:
                alert(f"Not saved: {self.__crontab} keeps changing or is missing")
                return
            if self.__crontab.is_saved():
                try:
                    self.__read()  # keep user's changes: the newer times are kept on re-read
                except FileNotFoundError:
                    pass  # gone again: try once more

    def __read(self) -> None:
        self.__tasks.read(self.__clock.time())
        self.__next_events = []

    def __write(self) -> bool:
        """Returns False if not saved: see `Crontab.save`"""
        if not self.__tasks.save():
            return False
        self.__checkpoint = self.__time
        self.__called_since_save = False
        return True

    def __execute(self, tasks: Iterable[Task]) -> None:
        if self.__tasks.execute(tasks, self.__clock.time()):
            self.__called_since_save = True

    def __next_tasks(self) -> list[Task]:
        """The tasks of the next event, once it is due. If Cronus is late (f.e. after a short system sleep),
        the tasks of all events due by now: each task runs once, as a skipped one, not once per missed event"""
        event = self.__next_event()
        if event.datetime > self.__clock.time():
            self.__wait(event.datetime)
        else:
            self.__time = event.datetime
        tasks = list(event.tasks)
        now = self.__clock.time()
        while self.__next_events and self.__next_events[0].datetime <= now:
            event = self.__next_events.pop(0)
            tasks += event.tasks
            self.__time = event.datetime
        return list(dict.fromkeys(tasks))  # each task once, in order

    def __next_event(self) -> Event:
        while not self.__next_events:
            self.__next_events = self.__determine_next_events()
            if not self.__next_events:
                self.__wait(must(self.__time) + self.__queue_interval)
        return self.__next_events.pop(0)

    def __determine_next_events(self) -> list[Event]:  # TODO: simplify (remove useless cycles)
        next_events_dic: dict[int, list[datetime]] = {}
        _time = must(self.__time)
        tasks = self.__tasks.by_line()
        for task_id, task in tasks.items():
            next_events_dic[task_id] = task.calls(_time, _time + self.__queue_interval)
        next_events_list: list[Event] = []
        while True:
            next_events_dic = {k: v for k, v in next_events_dic.items() if v}
            if not next_events_dic:
                break
            next_event_time = min(v[0] for v in next_events_dic.values())
            next_event_tasks: dict[int, Task] = {}
            for task_id in next_events_dic:
                if next_events_dic[task_id][0] == next_event_time:
                    # map by id in order by fix pre-midnight glitch (executing task twice)
                    next_event_tasks[task_id] = tasks[task_id]

                    del next_events_dic[task_id][0]
            next_events_list.append(Event(next_event_time, list(next_event_tasks.values())))
        return next_events_list

    def __last_checkpoint(self) -> datetime:
        return datetime.fromtimestamp(
            int(must(self.__time).timestamp() / self.__saving_interval.total_seconds())
            * self.__saving_interval.total_seconds())

    def __wait(self, until: datetime) -> None:
        saving_interval = self.__saving_interval if self.__called_since_save else self.__alive_saving_interval
        if must(self.__checkpoint) + saving_interval < until:
            self.__write()
        self.__sleep(until)
        self.__time = until

    def __sleep(self, until: datetime) -> None:
        _float: float = until.timestamp()
        while True:
            seconds_to_event = _float - self.__clock.time().timestamp()
            if seconds_to_event > 0:
                changed = self.__crontab.sleep_until_changed(min(seconds_to_event, self.__sleep_interval_seconds))
                self.__tasks.sync_watchdog()  # an ended task's pgid may be reused before the next event
                if changed and self.__tasks.is_crontab_changed():  # not just our own write
                    raise FileChangedException
            else:
                overdue = -seconds_to_event
                if overdue < self.__wakeup_interval_seconds:
                    return
                else:
                    raise WakeUpException

    def __update_time(self) -> None:
        self.__time = self.__clock.time()


stop_signals = (signal.SIGTERM, signal.SIGHUP)  # SIGHUP: f.e. the terminal is closed


def on_stop_signal(signum: int, _frame: Optional[FrameType]) -> None:  # signal handlers always get the frame
    # a second signal kills at once, without cleanup: then the watchdog stops the tasks
    for sig in stop_signals:
        signal.signal(sig, signal.SIG_DFL)
    raise SystemExit(128 + signum)


def main() -> None:
    if sys.argv[1:2] == [watchdog_option]:
        Watchdog.serve(timedelta(seconds=float(sys.argv[2])))
        sys.exit()
    watchers: dict[str, Callable[[str], file_watcher.FileWatcher]] = {
        'event': file_watcher.EventWatcher,  # needs `pip install watchdog`
        'polling': file_watcher.PollingWatcher,
    }
    parser = argparse.ArgumentParser(description='A cron daemon that also runs the tasks skipped while the machine was off')
    parser.add_argument('--watcher', choices=watchers, help='how to notice changes of the crontab')
    parser.add_argument('crontab')
    args = parser.parse_args()
    for stop_signal in stop_signals:  # by default, they kill without cleanup
        if signal.getsignal(stop_signal) is not signal.SIG_IGN:  # f.e. `nohup` ignores SIGHUP: keep it so
            signal.signal(stop_signal, on_stop_signal)
    crontab = Crontab(args.crontab,
                      watchers[args.watcher] if args.watcher else file_watcher.EventWatcher,
                      CrontabStore,  # StateFileStore
                      instance_lock.FlockLock)
    try:
        Cronus(crontab, Clock()).run()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as base_exception:
        alert(base_exception)
        raise


if __name__ == '__main__':
    main()
