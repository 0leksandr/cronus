from __future__ import annotations
from datetime import datetime, timedelta
from typing import Union, IO
import calendar
import os
import platform
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
last_call = ' #(\\d+|\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})'
end = '\\n?$'
# comment = '#(?:(?!' + last_call + end + ').)*'
comment = '#.*'
beginning = '^' + sep + '*'
pattern = beginning + (nr + sep + '+') * 6 + '(' + command + ')' + '(' + comment + ')?' + end


def alert(error):
    # print(traceback.format_exc())
    error = str(error).replace('"', '\\"')
    _traceback = sys.exc_info()[2]
    if _traceback:
        filename = os.path.split(_traceback.tb_frame.f_code.co_filename)[1]
        error += " at " + filename + ":" + str(_traceback.tb_lineno)
    subprocess.call('notify-send "' + Cronus.__name__ + ': ' + error + '"', shell=True)


class Clock:
    def time(self) -> datetime:
        return datetime.now()


last_call_fmt_timestamp = 1
last_call_fmt_datetime = 2


class LastCall:
    def __init__(self, _datetime: datetime, _format: int):
        self.datetime = _datetime
        self.format = _format

    @staticmethod
    def from_string(string: str):
        _last_call = re.search('^.+' + last_call + end, string)
        if _last_call:
            _last_call = _last_call.group(1)
            if _last_call.isdigit():
                return LastCall(datetime.fromtimestamp(int(_last_call)), last_call_fmt_timestamp)
            else:
                return LastCall(datetime.strptime(_last_call, '%Y-%m-%d %H:%M:%S'), last_call_fmt_datetime)
        return None

    def __str__(self) -> str:
        if self.format == last_call_fmt_timestamp:
            return str(int(self.datetime.timestamp()))
        if self.format == last_call_fmt_datetime:
            return self.datetime.strftime('%Y-%m-%d %H:%M:%S')

    def is_less(self, other: object) -> bool:
        if isinstance(other, LastCall):
            return self.datetime.timestamp() < other.datetime.timestamp()
        raise Exception(type(other))


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
                 _last_call: LastCall,
                 clock: Clock):
        self.__original_string = original_string
        self.__months = self.__values(months, 1, 12)
        self.__days = self.__values(days, 1, 31)
        self.__weekdays = self.__values(weekdays, 1, 7)
        if 7 in self.__weekdays:
            self.__weekdays.remove(7)
            self.__weekdays.insert(0, 0)
        self.__hours = self.__values(hours, 0, 23)
        self.__minutes = self.__values(minutes, 0, 59)
        self.__seconds = self.__values(seconds, 0, 59)
        self.__command = _command
        self.__last_call = _last_call
        self.__clock = clock
        self.__creation_time = clock.time()
        self.__expected_last_call(datetime(3000, 1, 1))  # todo: check if it may ever be called

    def __del__(self) -> None:
        process = self.__get_running_process()
        if process:
            process.terminate()
            process.wait(5)

    @staticmethod
    def from_string(string: str, clock: Clock) -> Union[Task, None]:
        global pattern, beginning, comment, last_call, end

        if re.match(beginning + comment, string) or re.match(beginning + end, string):
            return None
        task = re.search(pattern, string)
        if not task:
            raise Exception('Wrong format for task: ' + string)
        groups = task.groups()
        last_call_idx = 7
        if len(groups) != last_call_idx + 1:
            raise Exception('Task parsed incorrectly')
        _last_call = LastCall.from_string(string) or LastCall(clock.time(), last_call_fmt_datetime)
        string = re.match('^((?:(?!' + last_call + ').)*)(?:' + last_call + ')?' + end, string).group(1)
        # noinspection PyTypeChecker
        return Task(*([string] + [group.strip() for group in list(groups[:-1])] + [_last_call, clock]))

    def __str__(self) -> str:
        return self.__original_string + ' #' + str(self.__last_call)

    def skipped(self) -> bool:
        __last_call = self.__last_call.datetime if self.__last_call else self.__creation_time
        return __last_call < self.__expected_last_call()

    def calls(self, _from: datetime, _to: datetime) -> list[datetime]:
        _calls = []
        _from = max(_from, self.__last_call.datetime + timedelta(microseconds=1)) if self.__last_call else _from
        year = self.__year_start(_from)
        while True:
            for month in self.__months:  # todo: refactor
                __time = year.replace(month=month)
                if self.__add_month(__time) >= _from:
                    for day in self.__days:
                        if self.__is_correct_date(__time, day=day):
                            __time = __time.replace(day=day)
                            if __time + timedelta(days=1) >= _from:
                                for weekday in self.__weekdays:
                                    if self.__is_correct_date(__time, weekday=weekday):
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
            return
        self.__run()
        self.__set_last_call(self.__clock.time())

    def equals(self, other: object) -> bool:  # todo: cover with test
        if isinstance(other, Task):
            return self.__original_string == other.__original_string
        raise Exception(type(other))

    def get_last_call(self) -> LastCall:
        return self.__last_call

    def copy_last_call(self, other: object) -> None:
        if isinstance(other, Task):
            if other.__last_call:
                self.__set_last_call(other.__last_call.datetime)
        else:
            raise Exception(type(other))

    def __values(self, value: str, _min: int, _max: int) -> list[int]:
        values = sorted(list(set(self.__calc_values(value, _min, _max))))
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
                    value = [int(v) for v in match.groups()]
                    if value[1] <= value[0]:
                        raise Exception('Incorrect bounds')
                    values += list(range(value[0], value[1] + 1))
                else:
                    raise Exception('Unknown format')
        return values

    def __expected_last_call(self, now: datetime = None) -> datetime:
        if not now:
            now = self.__clock.time()
        years = 0
        year = self.__year_start(now)
        while True:
            for month in reversed(self.__months):  # todo: refactor
                __time = year.replace(month=month)
                if __time <= now:
                    for day in reversed(self.__days):
                        if self.__is_correct_date(__time, day=day):
                            __time = __time.replace(day=day)
                            if __time <= now:
                                for weekday in reversed(self.__weekdays):
                                    if self.__is_correct_date(__time, weekday=weekday):
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
        except Exception as exception:
            alert(exception)

    def __set_last_call(self, _datetime: datetime) -> None:
        if self.__last_call:
            self.__last_call.datetime = _datetime
        else:
            self.__last_call = LastCall(_datetime, 2)

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
    def __is_correct_date(base: datetime, day: int = None, weekday: int = None) -> bool:
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
    def __init__(self, _datetime: datetime, tasks: list[Task]):
        self.datetime = _datetime
        self.tasks = tasks


class Cronus:
    __file_watching_process: Union[subprocess.Popen, None]
    __tasks: dict[int, Task] = {}
    __next_events: list[Event] = []

    def __init__(self, filename: str, clock: Clock, sleep_interval_seconds: float = 5):
        self.__clock = clock  # workaround, because python's unittest cannot mock with lambda
        self.__queue_interval = timedelta(days=1)
        self.__wakeup_interval_seconds = timedelta(minutes=10).total_seconds()
        self.__saving_interval = timedelta(seconds=5)
        self.__sleep_interval_seconds = sleep_interval_seconds
        self.__filename = filename
        self.__file_watching_queue = queue.Queue()
        self.__lines = \
            self.__time = \
            self.__checkpoint = \
            self.__mtime = None

    def __del__(self):
        self.__write()
        for task in self.__tasks.values():
            task.__del__()
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
        t.start()

        self.__update_time()
        self.__checkpoint = self.__last_checkpoint()
        while True:
            try:
                old_tasks = self.__tasks
                self.__read()
                for task_id, task in self.__tasks.items():
                    for old_task in old_tasks.values():
                        if task.equals(old_task):
                            if task.get_last_call().is_less(old_task.get_last_call()):
                                self.__tasks[task_id].copy_last_call(old_task)
                self.__main_activity()
            except (FileChangedException, WakeUpException):
                continue
            except Exception:
                self.__del__()
                raise

    def __read(self) -> None:
        self.__tasks = {}
        self.__mtime = os.path.getmtime(self.__filename)
        with open(self.__filename, 'r') as file:
            self.__lines = file.readlines()
        for task_id in range(len(self.__lines)):
            task = None
            try:
                task = Task.from_string(self.__lines[task_id], self.__clock)
            except Exception as exception:
                alert(exception)
            if task:
                self.__tasks[task_id] = task

    def __write(self) -> None:
        if self.__tasks and self.__lines:
            new_lines = self.__lines[:]
            for task_id, task in self.__tasks.items():
                new_lines[task_id] = str(task) + '\n'
            if new_lines != self.__lines:
                with open(self.__filename, 'w') as file:
                    file.writelines(new_lines)
                while not self.__file_changed(1.):
                    pass
                self.__clear_file_changes_queue()
                self.__lines = new_lines
                self.__mtime = os.path.getmtime(self.__filename)
            self.__checkpoint = self.__time

    def __main_activity(self) -> None:
        self.__update_time()
        self.__run_skipped()
        self.__next_events = []
        while True:
            next_event = self.__next_event()
            if next_event.datetime > self.__clock.time():
                self.__wait(next_event.datetime)
            for task in next_event.tasks:
                task.execute()

    def __run_skipped(self) -> None:
        for task in self.__tasks.values():
            if task.skipped():
                task.execute()

    def __next_event(self) -> Event:
        while not self.__next_events:
            self.__next_events = self.__determine_next_events()
            if not self.__next_events:
                self.__wait(self.__time + self.__queue_interval)
        return self.__next_events.pop(0)

    def __determine_next_events(self) -> list[Event]:  # todo: simplify (remove useless cycles)
        next_events_dic = {}
        for task_id, task in self.__tasks.items():
            next_events_dic[task_id] = task.calls(self.__time, self.__time + self.__queue_interval)
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
            int(self.__time.timestamp() / self.__saving_interval.total_seconds())
            * self.__saving_interval.total_seconds())

    def __wait(self, until: datetime) -> None:
        next_checkpoint = self.__checkpoint + self.__saving_interval
        if next_checkpoint < until:
            self.__write()
        self.__sleep(until)
        self.__time = until

    def __sleep(self, until: datetime) -> None:
        until = until.timestamp()
        while True:
            seconds_to_event = until - self.__clock.time().timestamp()
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
