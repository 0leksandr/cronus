import re
from datetime import datetime, timedelta
import calendar
import subprocess
import time
from typing import List
import os
import sys

# todo: better track/check file change (akelpad)
# todo: check tasks to be unique?
# todo: save checkpoint is task was just executed, and next one is later then (next checkpoint)?
# todo: support for specific dates
# todo: why huge CPU load for XLS file, that is already open?
# todo: test daylight saving time
# external
# todo: remove time workaround when freezegun is fixed
# todo: push unittest-data-provider
# todo: linux app


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
    error = str(error).replace('"', '\\"')
    subprocess.call('notify-send "' + Cronus.__name__ + ': ' + error + '"', shell=True)


class Clock:
    def time(self):
        return datetime.now()


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
                return LastCall(datetime.fromtimestamp(int(_last_call)), 1)
            else:
                return LastCall(datetime.strptime(_last_call, '%Y-%m-%d %H:%M:%S'), 2)
        return None

    def __str__(self) -> str:
        if self.format == 1:
            return str(int(self.datetime.timestamp()))
        if self.format == 2:
            return self.datetime.strftime('%Y-%m-%d %H:%M:%S')


class Task:
    def __init__(self,
                 original_string: str,
                 months: str,
                 days: str,
                 weekdays: str,
                 hours: str,
                 minutes: str,
                 seconds: str,
                 _command: str,
                 clock: Clock,
                 _last_call: LastCall = None):
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
        self.__clock = clock
        self.__creation_time = clock.time()
        self.__last_call = _last_call
        self.__running = False
        self.__expected_last_call(datetime(3000, 1, 1))  # todo: check if it may ever be called

    @staticmethod
    def from_string(string: str, clock: Clock):
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
        _last_call = LastCall.from_string(string)
        string = re.match('^((?:(?!' + last_call + ').)*)(?:' + last_call + ')?' + end, string).group(1)
        return Task(*([string] + [group.strip() for group in list(groups[:-1])] + [clock, _last_call]))

    def __str__(self) -> str:
        global last_call, end

        string = self.__original_string
        match = re.search('^(.*)' + last_call + end, string) \
            or re.search('^(.*)' + end, string)
        if not match:
            raise Exception
        string = match.group(1)
        if self.__last_call:
            string += ' #' + str(self.__last_call)
        return string

    def skipped(self) -> bool:
        __last_call = self.__last_call.datetime if self.__last_call else self.__creation_time
        return __last_call < self.__expected_last_call()

    def calls(self, _from: datetime, _to: datetime) -> List[datetime]:
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
        if not self.__running:
            self.__running = True
            self.__run()
            if self.__last_call:
                self.__last_call.datetime = self.__clock.time()
            else:
                self.__last_call = LastCall(self.__clock.time(), 1)
            self.__running = False

    def equals(self, other: object) -> bool:  # todo: cover with test
        if isinstance(other, Task):
            return self.__original_string == other.__original_string
        raise Exception(type(other))

    def __values(self, value: str, _min: int, _max: int) -> List[int]:
        values = sorted(list(set(self.__calc_values(value, _min, _max))))
        for v in values:
            if not _min <= v <= _max:
                raise Exception
        return values

    @staticmethod
    def __calc_values(value: str, _min: int, _max: int) -> List[int]:
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
                else:
                    match = re.search('(\\d+)-(\\d+)', value)
                    if match:
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
            subprocess.call(self.__command, shell=True)
        except Exception as exception:
            alert(exception)

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
    def __init__(self, _datetime: datetime, task_ids: List[int]):
        self.datetime = _datetime
        self.task_ids = task_ids


class Cronus:
    def __init__(self, clock: Clock):
        self.__clock = clock  # fucking workaround because python's unittest cannot mock with lambda
        self.__queue_interval = timedelta(days=1)
        self.__wakeup_interval = timedelta(minutes=10).total_seconds()
        self.__saving_interval = timedelta(minutes=5)
        self.__sleep_interval = timedelta(seconds=0.5).total_seconds()
        self.__filename = \
            self.__tasks = \
            self.__lines = \
            self.__next_events = \
            self.__time = \
            self.__checkpoint = \
            self.__mtime = None

    def __del__(self):
        self.__write()

    def run(self, filename: str) -> None:
        self.__filename = filename
        self.__update_time()
        self.__read()
        self.__checkpoint = self.__last_checkpoint()
        self.__main_activity()

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
                self.__check_file()
                with open(self.__filename, 'w') as file:
                    file.writelines(new_lines)
                self.__lines = new_lines
                self.__mtime = os.path.getmtime(self.__filename)
            self.__checkpoint = self.__time

    def __main_activity(self) -> None:
        self.__update_time()
        self.__run_skipped()
        self.__next_events = []
        tasks = []
        try:
            while True:
                next_event = self.__next_event()
                if next_event.datetime > self.__clock.time():
                    for task in tasks:
                        task.execute()
                    tasks = []
                    self.__wait(next_event.datetime)
                for task_id in next_event.task_ids:
                    tasks.append(self.__tasks[task_id])
        except FileChangedException:
            old_tasks = self.__tasks
            self.__read()
            for task_id, task in self.__tasks.items():
                for old_task in old_tasks.values():
                    if task.equals(old_task):
                        self.__tasks[task_id] = old_task
            self.__main_activity()
        except WakeUpException:
            self.__main_activity()

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

    def __determine_next_events(self) -> List[Event]:
        next_events_dic = {}
        for task_id, task in self.__tasks.items():
            next_events_dic[task_id] = task.calls(self.__time, self.__time + self.__queue_interval)
        next_events_list = []
        while True:
            next_events_dic = {k: v for k, v in next_events_dic.items() if v}
            if not next_events_dic:
                break
            next_event_time = min(v[0] for v in next_events_dic.values())
            next_task_ids = []
            for task_id in next_events_dic:
                if next_events_dic[task_id][0] == next_event_time:
                    next_task_ids.append(task_id)
                    del next_events_dic[task_id][0]
            # next_events_list.append((next_event_time, next_task_ids))
            next_events_list.append(Event(next_event_time, next_task_ids))
        return next_events_list

    def __last_checkpoint(self) -> datetime:
        return datetime.fromtimestamp(
            int(self.__time.timestamp() / self.__saving_interval.total_seconds())
            * self.__saving_interval.total_seconds())

    def __wait(self, _datetime: datetime) -> None:
        next_checkpoint = self.__checkpoint + self.__saving_interval
        if next_checkpoint < _datetime:
            self.__write()
        self.__sleep(_datetime)
        self.__time = _datetime

    def __sleep(self, until: datetime) -> None:
        until = until.timestamp()
        while True:
            self.__check_file()
            diff = until - self.__clock.time().timestamp()
            if diff > 0:
                time.sleep(min((diff, self.__sleep_interval)))
            elif -diff < self.__wakeup_interval:
                return
            else:
                raise WakeUpException

    def __check_file(self) -> None:
        if os.path.getmtime(self.__filename) != self.__mtime:
            raise FileChangedException

    def __update_time(self) -> None:
        self.__time = self.__clock.time()


if __name__ == '__main__':
    try:
        Cronus(Clock()).run(sys.argv[1])
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as base_exception:
        alert(base_exception)
        raise
