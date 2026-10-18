from __future__ import annotations
from datetime import datetime, timedelta
from typing import Union, IO, Optional, TypeVar
import calendar
import gc
import os
import platform
import psutil
import queue
import re
import subprocess
import sys
import threading

# todo: allow manual decrease of last_call
# todo: better track/check file change (akelpad)
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
    error = str(error).replace('"', '\\"')
    _traceback = sys.exc_info()[2]
    if _traceback:
        filename = os.path.split(_traceback.tb_frame.f_code.co_filename)[1]
        error += f" at {filename}:{_traceback.tb_lineno}"
    subprocess.call(f"alert \"{Cronus.__name__}: {error}\"", shell=True)


T = TypeVar('T')
def must(a: Optional[T]) -> T:
    assert a is not None
    return a


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
        self.format = _format
        self.called_at = called_at
        self.alive_at = alive_at  # last time the process started at `called_at` was seen running

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

    def is_less(self, other: Status) -> bool:
        return self.called_at.timestamp() < other.called_at.timestamp()


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
                 _status: Status,
                 clock: Clock) -> None:
        self.__original_string = original_string
        self.__months = Task.__values(months, 1, 12)
        self.__days = Task.__values(days, 1, 31)
        self.__weekdays = Task.__values(weekdays, 1, 7)
        if 7 in self.__weekdays:
            self.__weekdays.remove(7)
            self.__weekdays.insert(0, 0)
        self.__hours = Task.__values(hours, 0, 23)
        self.__minutes = Task.__values(minutes, 0, 59)
        self.__seconds = Task.__values(seconds, 0, 59)
        self.__command = _command
        self.__status = _status
        self.__clock = clock
        self.__expected_last_call(datetime(3000, 1, 1))  # todo: check if it may ever be called

    def __del__(self) -> None:
        if process := self.__get_running_process():
            # process.terminate()
            # process.wait(5)

            # try:
            #     os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            # except ProcessLookupError:
            #     pass
            # process.wait(5)

            try:
                parent = psutil.Process(process.pid)
                children = parent.children(recursive=True)

                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                parent.terminate()

                gone, alive = psutil.wait_procs(children + [parent], timeout=5)

                for p in alive:
                    try:
                        p.kill()
                    except psutil.NoSuchProcess:
                        pass
            except psutil.NoSuchProcess:
                pass

    @staticmethod
    def from_string(string: str, clock: Clock) -> Union[Task, None]:
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
        _status = Status.from_string(string) or Status(status_fmt_datetime, clock.time())
        string = must(re.match('^((?:(?!' + status + ').)*)(?:' + status + ')?' + end, string)).group(1)
        # pyrefly: ignore[bad-argument-type]
        return Task(*([string] + [group.strip() for group in list(groups[:-1])] + [_status, clock]))

    def __str__(self) -> str:
        return self.__original_string + ' #' + self.__status.__str__()

    def is_skipped(self) -> bool:
        __last_call = self.__status.alive_at or self.__status.called_at
        return __last_call < self.__expected_last_call()

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

    def execute(self) -> None:
        if self.__get_running_process():
            self.__status.alive_at = self.__clock.time()
            return
        self.__run()
        self.__set_called_at(self.__clock.time())
        self.__status.alive_at = None

    def equals(self, other: Task) -> bool:  # todo: cover with test
        return self.__original_string == other.__original_string

    def copy_status(self, other: Task) -> None:
        if self.__status.is_less(other.__status):
            self.__set_called_at(other.__status.called_at)
            self.__status.alive_at = other.__status.alive_at

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

    def __expected_last_call(self, now: Optional[datetime] = None) -> datetime:
        if not now:
            now = self.__clock.time()
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
            self.__process = subprocess.Popen(self.__command, shell=True)
            # self.__process = subprocess.Popen(self.__command, env=os.environ)
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
        self.datetime = _datetime
        self.tasks = tasks


class Cronus:
    __file_watching_process: Union[subprocess.Popen, None]
    __tasks: dict[int, Task] = {}
    __next_events: list[Event] = []
    __lines: list[str] = []

    def __init__(self,
                 filename: str,
                 clock: Clock,
                 sleep_interval_seconds: float = 5) -> None:
        self.__clock = clock  # workaround, because python's unittest cannot mock with lambda
        self.__queue_interval = timedelta(days=1)
        self.__wakeup_interval_seconds = timedelta(minutes=10).total_seconds()
        self.__saving_interval = timedelta(seconds=5)
        self.__sleep_interval_seconds = sleep_interval_seconds
        self.__filename = filename
        self.__file_watching_queue = queue.Queue()
        self.__time: Optional[datetime] = None
        self.__checkpoint: Optional[datetime] = None

    def __del__(self) -> None:
        self.__write()
        if self.__file_watching_process:
            self.__file_watching_process.terminate()
            self.__file_watching_process.wait(5)

    def run(self) -> None:
        match system := platform.system():
            case "Linux":
                self.__file_watching_process = subprocess.Popen(("inotifywait",
                                                                 "--monitor",
                                                                 "--event",
                                                                 "CLOSE_WRITE",
                                                                 self.__filename),
                                                                stdout=subprocess.PIPE)
            case "Darwin":
                self.__file_watching_process = subprocess.Popen(("fswatch",
                                                                 "--one-per-batch",
                                                                 "--event",
                                                                 "Updated",
                                                                 self.__filename),
                                                                stdout=subprocess.PIPE)
            case _:
                raise Exception(f"Unknown OS: {system}")

        def enqueue_output(out: IO, _queue: queue.Queue) -> None:
            for line in iter(out.readline, b''):
                _queue.put(line)
            out.close()

        t = threading.Thread(target=enqueue_output,
                             args=(self.__file_watching_process.stdout, self.__file_watching_queue))
        t.daemon = True
        t.start()  # MAYBE: kill in __del__

        self.__update_time()
        self.__checkpoint = self.__last_checkpoint()
        while True:
            try:
                self.__read()
                gc.collect()  # PyPy has no refcounting: force collection so old tasks' __del__ runs
                self.__update_time()
                self.__run_skipped()
                while True:
                    next_event = self.__next_event()
                    if next_event.datetime > self.__clock.time():
                        self.__wait(next_event.datetime)
                    for task in next_event.tasks:
                        task.execute()
            except (FileChangedException, WakeUpException):
                continue
            except Exception:
                self.__del__()
                raise
            finally:
                # noinspection unused-local
                next_event = task = None  # Release references to old tasks so their __del__ can run

    def __read(self) -> None:
        new_tasks: dict[int, Task] = {}
        with open(self.__filename, 'r') as file:
            self.__lines = file.readlines()
        for task_id in range(len(self.__lines)):
            try:
                if task := Task.from_string(self.__lines[task_id], self.__clock):
                    new_tasks[task_id] = task
            except Exception as exception:
                alert(exception)

        old_tasks = list(self.__tasks.values())
        updated_tasks: dict[int, Task] = {}
        for new_task_id, new_task in new_tasks.items():
            for old_task in old_tasks:
                if new_task.equals(old_task):
                    old_tasks.remove(old_task)
                    old_task.copy_status(new_task)
                    new_task = old_task
                    break
            updated_tasks[new_task_id] = new_task
        self.__tasks = updated_tasks

        self.__next_events = []

    def __write(self) -> None:
        if self.__tasks and self.__lines:
            new_lines = self.__lines[:]
            for task_id, task in self.__tasks.items():
                new_lines[task_id] = task.__str__() + '\n'
            if new_lines != self.__lines:
                with open(self.__filename, 'w') as file:
                    file.writelines(new_lines)

                # while not self.__file_changed(1.): # sometimes gets stuck on start
                #     pass
                self.__file_changed(1.)

                self.__clear_file_changes_queue()
                self.__lines = new_lines
            self.__checkpoint = self.__time

    def __run_skipped(self) -> None:
        for task in self.__tasks.values():
            if task.is_skipped():
                task.execute()

    def __next_event(self) -> Event:
        while not self.__next_events:
            self.__next_events = self.__determine_next_events()
            if not self.__next_events:
                self.__wait(must(self.__time) + self.__queue_interval)
        return self.__next_events.pop(0)

    def __determine_next_events(self) -> list[Event]:  # TODO: simplify (remove useless cycles)
        next_events_dic = {}
        _time = must(self.__time)
        for task_id, task in self.__tasks.items():
            next_events_dic[task_id] = task.calls(_time, _time + self.__queue_interval)
        next_events_list = []
        while True:
            next_events_dic = {k: v for k, v in next_events_dic.items() if v}
            if not next_events_dic:
                break
            next_event_time = min(v[0] for v in next_events_dic.values())
            next_event_tasks = {}
            for task_id in next_events_dic:
                if next_events_dic[task_id][0] == next_event_time:
                    # map by id in order by fix pre-midnight glitch (executing task twice)
                    next_event_tasks[task_id] = self.__tasks[task_id]

                    del next_events_dic[task_id][0]
            next_events_list.append(Event(next_event_time, list(next_event_tasks.values())))
        return next_events_list

    def __last_checkpoint(self) -> datetime:
        return datetime.fromtimestamp(
            int(must(self.__time).timestamp() / self.__saving_interval.total_seconds())
            * self.__saving_interval.total_seconds())

    def __wait(self, until: datetime) -> None:
        next_checkpoint = must(self.__checkpoint) + self.__saving_interval
        if next_checkpoint < until:
            self.__write()
        self.__sleep(until)
        self.__time = until

    def __sleep(self, until: datetime) -> None:
        _float: float = until.timestamp()
        while True:
            seconds_to_event = _float - self.__clock.time().timestamp()
            if seconds_to_event > 0:
                self.__watch_file(min((seconds_to_event, self.__sleep_interval_seconds)))
            else:
                overdue = -seconds_to_event
                if overdue < self.__wakeup_interval_seconds:
                    return
                else:
                    raise WakeUpException

    def __watch_file(self, timeout_seconds: float) -> None:
        if self.__file_changed(timeout_seconds):
            self.__clear_file_changes_queue()
            raise FileChangedException

    def __file_changed(self, timeout_seconds: float) -> bool:
        try:
            self.__file_watching_queue.get(True, timeout_seconds)
            return True
        except queue.Empty:
            return False

    def __clear_file_changes_queue(self) -> None:
        while not self.__file_watching_queue.empty():
            self.__file_watching_queue.queue.clear()

    def __update_time(self) -> None:
        self.__time = self.__clock.time()


if __name__ == '__main__':
    try:
        Cronus(sys.argv[1], Clock()).run()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as base_exception:
        alert(base_exception)
        raise
