from __future__ import annotations
from datetime import datetime
from typing import Callable
import importlib.util
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import cronus
from cronus import Cronus, Clock, Crontab, CrontabStore, StateFileStore, StatusStore
import file_watcher
import instance_lock


stop_timeout_seconds = 5  # Tasks.__stop_timeout


class StoppableClock(Clock):
    """Real time. Once stopped, raises SystemExit in the daemon's thread, as SIGTERM does in `__main__`"""

    def __init__(self) -> None:
        self.stopped = False

    def time(self) -> datetime:
        if self.stopped:
            raise SystemExit
        return super().time()


class ManualClock(StoppableClock):
    """Stands still until the test moves it: the test decides when Cronus runs a task, and so when it saves"""

    def __init__(self, now: datetime) -> None:
        super().__init__()
        self.now = now

    def time(self) -> datetime:
        super().time()  # raises once stopped
        return self.now


class TestRobustness(unittest.TestCase):
    """
    The paths that keep the tasks and the crontab safe: the lock, the file check, the save, the wake-up,
    the watchdog, the shutdown. Each test in its own temporary directory. Takes about half a minute.
    The long tasks run once a year, and their last call is 1970: each runs at once as skipped, and Cronus has
    no later events, so it never saves during a test on its own.
    The file check interval of a test is the settle interval of the event watcher, in `TestRobustnessWithEvents`.
    Most tests run in real time. The tests of a save or a wake-up use a `ManualClock`: the test opens a gap
    (a half-written or missing file), keeps it open, and moves the clock to make Cronus save or wake up in it
    """

    watcher_class: type[file_watcher.FileWatcher] = file_watcher.PollingWatcher
    watcher_option = 'polling'  # the same watcher for a real process: `python cronus.py --watcher <option>`

    def setUp(self) -> None:
        self.__dir = tempfile.TemporaryDirectory()
        self.__crontab = os.path.join(self.__dir.name, 'crontab')
        self.__lock = os.path.realpath(self.__crontab) + '.lock'
        self.__pids = os.path.join(self.__dir.name, 'pids')
        self.__ticks = os.path.join(self.__dir.name, 'ticks')
        self.__daemons: list[tuple[threading.Thread, StoppableClock]] = []
        self.__processes: list[subprocess.Popen] = []

    def tearDown(self) -> None:
        for thread, clock in self.__daemons:
            clock.stopped = True
            thread.join(stop_timeout_seconds + 5)
        for process in self.__processes:
            if process.poll() is None:
                process.terminate()  # Cronus stops its tasks itself
                try:
                    process.wait(stop_timeout_seconds + 5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        # the groups go once their zombies are reaped, f.e. by the watchdog's stop after a SIGKILL
        left = [pgid for pgid in self.__read_pids() if not self.__is_gone(pgid, stop_timeout_seconds + 2)]
        for pgid in left:  # nothing may be left running, even after a failed test
            os.killpg(pgid, signal.SIGKILL)
        self.__dir.cleanup()
        assert not left, f'Cronus left process groups running: {left}'

    def test_second_instance_is_refused(self):
        self.__write(self.__long_task())
        self.__start_daemon()
        pgid = self.__wait_for_pids(1)[0]

        with self.assertRaisesRegex(Exception, 'Already running'):
            Cronus(Crontab(self.__crontab, file_watcher.PollingWatcher, CrontabStore, instance_lock.FlockLock), StoppableClock()).run()
        assert self.__is_alive(pgid)  # the first instance is not disturbed

        other_crontab = os.path.join(self.__dir.name, 'other')
        self.__write(self.__long_task('sleep 301'), other_crontab)
        self.__start_daemon(other_crontab)  # another crontab may have its own instance
        self.__wait_for_pids(2)

    def test_lock_file_is_deleted_on_exit(self):
        self.__write(self.__long_task())
        thread, clock = self.__start_daemon()
        pgid = self.__wait_for_pids(1)[0]
        assert os.path.exists(self.__lock)

        clock.stopped = True
        thread.join(stop_timeout_seconds + 5)
        assert not thread.is_alive()
        assert not os.path.exists(self.__lock)
        assert not self.__is_alive(pgid)

    def test_sigterm_saves_and_stops(self):
        self.__write(self.__long_task())
        process = self.__start_process()
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.2)  # the task's shell records its pid after Cronus starts it

        process.send_signal(signal.SIGTERM)
        assert process.wait(stop_timeout_seconds + 5) == 128 + signal.SIGTERM
        assert not self.__is_alive(pgid)
        assert not os.path.exists(self.__lock)
        assert not self.__read().endswith(' #0\n')  # the call is saved

    def test_stop_is_parallel_and_kills_who_ignores_sigterm(self):
        ignore_sigterm = "trap '' TERM; "  # the shell and its sleep both ignore it
        self.__write(self.__long_task('sleep 300', ignore_sigterm)
                     + self.__long_task('sleep 301', ignore_sigterm))
        process = self.__start_process()
        pgids = self.__wait_for_pids(2)
        time.sleep(0.2)

        started_at = time.monotonic()
        process.send_signal(signal.SIGTERM)
        process.wait(2 * stop_timeout_seconds + 5)
        elapsed = time.monotonic() - started_at
        assert elapsed >= stop_timeout_seconds - 0.5, elapsed  # SIGTERM was ignored: SIGKILL after the timeout
        assert elapsed < stop_timeout_seconds + 3, elapsed  # one timeout for both tasks, not one per task
        for pgid in pgids:
            assert not self.__is_alive(pgid)

    def test_kill_9_stops_tasks_by_watchdog(self):
        self.__write(self.__long_task())
        process = self.__start_process()
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.2)  # Cronus registers the group with the watchdog right after it starts the task

        process.kill()
        process.wait()
        self.__wait_until(lambda: not self.__is_alive(pgid), stop_timeout_seconds + 2, 'the task is still running')

        assert os.path.exists(self.__lock)  # left behind by SIGKILL...
        self.__write(self.__read() + self.__long_task('sleep 301'))
        self.__start_process()
        self.__wait_for_pids(2)  # ...and harmless: the next instance runs

    def test_half_written_file_is_not_read(self):
        self.__write(self.__long_task())
        check_interval_seconds = 0.2
        self.__start_daemon(file_check_interval_seconds=check_interval_seconds)
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.5)  # Cronus has saved the call
        content = self.__read()

        with open(self.__crontab, 'w') as file:  # in place, as some editors save: truncate, then write
            file.flush()
            time.sleep(check_interval_seconds * 0.75)  # empty for less than one interval: one check at most
            file.write(content)
        time.sleep(check_interval_seconds * 5)

        assert self.__is_alive(pgid)  # an empty read would have stopped it
        assert self.__read_pids() == [pgid]  # and then run it again as skipped

    def test_change_is_followed(self):
        self.__write(self.__long_task())
        self.__start_daemon(file_check_interval_seconds=0.2)
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.5)

        self.__write(self.__read() + self.__long_task('sleep 301'))  # in place
        self.__wait_for_pids(2)  # the new task runs
        assert self.__is_alive(pgid)  # the unchanged task keeps its process

    def test_save_by_rename_is_followed(self):
        self.__write(self.__long_task())
        thread, _ = self.__start_daemon()
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.5)
        content = self.__read()

        os.rename(self.__crontab, self.__crontab + '~')  # as some editors save: the path is missing for a while
        time.sleep(0.3)  # several checks
        self.__write(content + self.__long_task('sleep 301'))

        self.__wait_for_pids(2)  # the new task runs
        assert thread.is_alive()
        assert self.__is_alive(pgid)  # the unchanged task keeps its process

    def test_save_into_half_written_file(self):
        """The save, not the file check, finds the file half-written: it must not re-read it"""
        thread, clock, pgid, content = self.__start_saving_daemon()

        with open(self.__crontab, 'w') as file:  # truncated, and stays empty while Cronus saves
            self.__run_minutely_task(clock)
            file.write(content)

        assert thread.is_alive()
        assert self.__is_alive(pgid)  # an empty read would have stopped it
        assert self.__read_pids() == [pgid]  # and then run it again as skipped

    def test_save_into_rename(self):
        """The save, not the file check, finds the file missing, as during a save by rename"""
        thread, clock, pgid, content = self.__start_saving_daemon()

        os.rename(self.__crontab, self.__crontab + '~')
        try:
            self.__run_minutely_task(clock)
            assert thread.is_alive()  # FileNotFoundError would have ended it
        finally:
            os.rename(self.__crontab + '~', self.__crontab)
        assert self.__is_alive(pgid)
        assert self.__read() == content  # the save did not write over the rename

    def test_wake_up_into_rename(self):
        """After a wake-up, Cronus re-reads the file: it must wait while the file is missing"""
        clock = ManualClock(datetime(2017, 11, 18, 12, 0, 30))
        self.__write(self.__long_task() + self.__minutely_task())
        thread, _ = self.__start_daemon(file_check_interval_seconds=0.05, clock=clock)
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.5)

        os.rename(self.__crontab, self.__crontab + '~')
        try:
            clock.now = datetime(2017, 11, 18, 12, 30, 0)  # more than 10 minutes late: a wake-up
            time.sleep(0.5)
            assert thread.is_alive()  # FileNotFoundError would have ended it
        finally:
            os.rename(self.__crontab + '~', self.__crontab)

        self.__wait_until(lambda: os.path.exists(self.__ticks), 5, 'Cronus did not resume after the wake-up')
        assert self.__is_alive(pgid)
        assert self.__read_pids() == [pgid]

    def test_state_file_leaves_the_crontab_untouched(self):
        content = self.__long_task()
        self.__write(content)
        state = self.__crontab + '.state'
        thread, clock = self.__start_daemon(store=StateFileStore)
        self.__wait_for_pids(1)

        clock.stopped = True
        thread.join(stop_timeout_seconds + 5)
        assert self.__read() == content  # never written: the line's `#0` stays, though stale
        assert os.path.exists(state)

        self.__start_daemon(store=StateFileStore)
        time.sleep(1)
        assert len(self.__read_pids()) == 1  # the call saved in the state file wins over the line's `#0`

    def test_late_events_run_once(self):
        """Late by less than a wake-up (f.e. a short system sleep): each task runs once, not once per missed event"""
        clock = ManualClock(datetime(2017, 11, 18, 12, 0, 0, 500000))
        self.__write(f"* * * * * * echo tick >> {self.__ticks} #2017-11-18 12:00:00\n")  # every second
        self.__start_daemon(clock=clock)
        time.sleep(0.5)

        clock.now = datetime(2017, 11, 18, 12, 9, 0, 500000)  # 540 events late, but less than 10 minutes
        time.sleep(1)

        with open(self.__ticks) as file:
            assert len(file.readlines()) == 1

    def __start_saving_daemon(self) -> tuple[threading.Thread, ManualClock, int, str]:
        """Cronus with a clock that stands still, and a file check too rare to notice the test's file changes:
        only a save can find them, and it saves only when the test moves the clock"""
        clock = ManualClock(datetime(2017, 11, 18, 12, 0, 30))
        self.__write(self.__long_task() + self.__minutely_task())
        thread, _ = self.__start_daemon(file_check_interval_seconds=10, clock=clock)
        pgid = self.__wait_for_pids(1)[0]
        time.sleep(0.5)  # Cronus has saved the long task's call
        return thread, clock, pgid, self.__read()

    def __run_minutely_task(self, clock: ManualClock) -> None:
        """Cronus runs the task, then saves right before it sleeps until the next minute"""
        clock.now = datetime(2017, 11, 18, 12, 1, 0)
        self.__wait_until(lambda: os.path.exists(self.__ticks), 5, 'the minutely task did not run')
        time.sleep(0.5)

    def __minutely_task(self) -> str:
        """Called at 12:00:00, so not skipped at 12:00:30"""
        return f"* * * * * 0 echo tick >> {self.__ticks} #2017-11-18 12:00:00\n"

    def __long_task(self, command: str = 'sleep 300', prefix: str = '') -> str:
        """Records its process group (its shell's pid). Runs at once as skipped, then once a year"""
        return f"1 1 * 0 0 0 {prefix}echo $$ >> {self.__pids}; {command} #0\n"

    def __start_daemon(self,
                       crontab: str | None = None,
                       file_check_interval_seconds: float = 0.05,
                       clock: StoppableClock | None = None,
                       store: Callable[[str], StatusStore] = CrontabStore) -> tuple[threading.Thread, StoppableClock]:
        clock = clock or StoppableClock()
        daemon = Cronus(Crontab(crontab or self.__crontab,
                                lambda path: self.watcher_class(path, file_check_interval_seconds),
                                store,
                                instance_lock.FlockLock),
                        clock,
                        0.05)
        thread = threading.Thread(target=daemon.run, daemon=True)
        thread.start()
        self.__daemons.append((thread, clock))
        return thread, clock

    def __start_process(self) -> subprocess.Popen:
        """For signals: a real `python cronus.py --watcher <option> <crontab>`, with the test's watcher"""
        process = subprocess.Popen((sys.executable, cronus.must(cronus.__file__), '--watcher', self.watcher_option, self.__crontab))
        self.__processes.append(process)
        return process

    def __wait_for_pids(self, count: int) -> list[int]:
        self.__wait_until(lambda: len(self.__read_pids()) >= count, 5, f'{count} task(s) did not start')
        return self.__read_pids()

    def __read_pids(self) -> list[int]:
        if not os.path.exists(self.__pids):
            return []
        with open(self.__pids) as file:
            return [int(line) for line in file.readlines() if line.endswith('\n')]

    @staticmethod
    def __is_alive(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
            return True
        except PermissionError:  # macOS: only zombies are left, until their parent reaps them
            return True
        except ProcessLookupError:
            return False

    @staticmethod
    def __is_gone(pgid: int, timeout_seconds: float) -> bool:
        """Waits until the group is gone. On macOS, its zombies keep it until their parent (or launchd) reaps them"""
        deadline = time.monotonic() + timeout_seconds
        while TestRobustness.__is_alive(pgid):
            if time.monotonic() > deadline:
                return False
            time.sleep(0.05)
        return True

    @staticmethod
    def __wait_until(condition: Callable[[], bool], timeout_seconds: float, message: str) -> None:
        deadline = time.monotonic() + timeout_seconds
        while not condition():
            if time.monotonic() > deadline:
                raise AssertionError(message)
            time.sleep(0.05)

    def __read(self) -> str:
        with open(self.__crontab) as file:
            return file.read()

    def __write(self, content: str, filename: str | None = None) -> None:
        with open(filename or self.__crontab, 'w') as file:
            file.write(content)


@unittest.skipIf(importlib.util.find_spec('watchdog') is None, 'needs `pip install watchdog`')
class TestRobustnessWithEvents(TestRobustness):
    """The same tests with the event watcher, also in a real process"""
    watcher_class = file_watcher.EventWatcher
    watcher_option = 'event'

