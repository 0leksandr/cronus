import unittest
from unittest_data_provider import data_provider
from datetime import datetime, timedelta
from dateutil.parser import parse
from typing import List, Tuple
# from freezegun import freeze_time

# import sys
# import os
# sys.path.append(os.path.abspath(".."))
from cronus import Task, Clock


class MockClock(Clock):
    def __init__(self, time: datetime):
        self.__time = time

    def time(self):
        return self.__time


class TestTask(unittest.TestCase):
    __command = 'echo > /dev/null'

    correct_commands = (__command,
                        __command + ' "a"',
                        __command + " 'b'",
                        __command + ' "#c"',
                        __command + ' \'#d \' "#e " \'# f\' "# g"')

    def correct_strings(self) -> List[str]:
        correct_beginnings = correct_delimiters = (
            # ' ',
            # '\t',
            '\t \t \t\t   \t ',
        )
        correct_dates = ('* *',
                         '* 1',
                         '1 *',
                         '*/3 31',
                         '* 1-15',
                         '2 29')
        correct_weekdays = (
            '*',
            '1',
            '7',
            # '*/1',
            # '*/2',
            # '*/7',
        )
        correct_hours = ('*',
                         '0',
                         # '23',
                         # '*/1',
                         '*/23')
        correct_minutes = correct_seconds = ('*',
                                             '0',
                                             # '59',
                                             # '*/1',
                                             '*/59')
        correct_comments = ('',
                            ' # comment',
                            '#comment "abc"')
        for beginning in correct_beginnings:
            for delimiter in correct_delimiters:
                for date in correct_dates:
                    for weekday in correct_weekdays:
                        for hour in correct_hours:
                            for minute in correct_minutes:
                                for second in correct_seconds:
                                    for command in self.correct_commands:
                                        for comment in correct_comments:
                                            yield beginning + date + delimiter + weekday + \
                                                  delimiter + hour + delimiter + minute + \
                                                  delimiter + second + delimiter + command + comment

    empty_strings = ('',
                     '# comment',
                     '#' + __command)

    incorrect_strings = ('* * * * * ' + __command,
                         '0 * * * * * ' + __command,
                         '13 * * * * * ' + __command,
                         '*/0 * * * * * ' + __command,
                         '*/13 * * * * * ' + __command,
                         '* 0 * * * * ' + __command,
                         '* 32 * * * * ' + __command,
                         '* */0 * * * * ' + __command,
                         '* */32 * * * * ' + __command,
                         '* * 0 * * * ' + __command,
                         '* * 8 * * * ' + __command,
                         '* * */0 * * * ' + __command,
                         '* * */8 * * * ' + __command,
                         '* * * 24 * * ' + __command,
                         '* * * * 60 * ' + __command,
                         '* * * * * 60 ' + __command,
                         '* * * * * * ' + __command + '"',
                         '2 30 * * * * ' + __command,
                         '4 31 * * * * ' + __command)

    skipped_provider = (('* * * 22 30 00',    '2017-11-17 13:24', '2017-11-16 22:30'),
                        ('* * 1 12 00 00',    '2018-01-01 11:59', '2017-12-25 12:00'),
                        ('* * 1 12 00 00',    '2018-01-01 12:00', '2017-12-25 12:00'),
                        ('* * 1 12 00 00',    '2018-01-01 12:01', '2018-01-01 12:00'),
                        ('* 29 * 1 2 3',      '2018-03-01',       '2018-01-29 01:02:03'),
                        ('* 29 * 1 2 3',      '2020-03-01',       '2020-02-29 01:02:03'),
                        ('*/2 29 * 1 2 3',    '2018-03-01',       '2017-12-29 01:02:03'),
                        ('*/2 29 * 1 2 3',    '2020-03-01',       '2020-02-29 01:02:03'),
                        ('*/4 31 * 1 2 3',    '2017-08-01',       '2016-12-31 01:02:03'),
                        ('*/4 31 * 1 2 3',    '2017-09-01',       '2017-08-31 01:02:03'),
                        ('*/4 31 * 1 2 3',    '2017-10-01',       '2017-08-31 01:02:03'),
                        ('*/4 31 * 1 2 3',    '2017-11-01',       '2017-08-31 01:02:03'),
                        ('* * * 5 * *',       '2017-11-19 20:50', '2017-11-19 05:59:59'),
                        ('1-3 1-3 1-3 0 0 0', '2017-11-19',       '2017-03-01'),
                        ('1-2 1-2 1-2 0 0 0', '2017-11-19',       '2017-01-02'),
                        ('1,2 1,2 1,2 0 0 0', '2017-11-19',       '2017-01-02'),
                        ('1 1 1 0 0 0',       '2017-11-19',       '2007-01-01'))

    calls_provider = [
        ('* * * 22 30 00', None, '2017-11-16', '2017-11-17', {0: '2017-11-16 22:30'}),
        ('* * */2 03 2 *',
         None,
         '2017-11-16',
         '2017-11-30 12:02',
         {0: '2017-11-16 03:02:00', 59: '2017-11-16 03:02:59', 60: '2017-11-18 03:02:00',
          119: '2017-11-18 03:02:59', 120: '2017-11-21 03:02:00', 419: '2017-11-30 03:02:59'}),
        ('* * * * * *', None, '2018-12-31 23:59:59', '2018-01-01 00:00:00', {}),
        ('* * * 22 15 00',
         None,
         '2017-11-19 10:17:45.194909',
         '2017-11-20 10:17:45.194909',
         {0: '2017-11-19 22:15'}),
        ('* * * * * *',
         None,
         '2017-11-19 12:43:25',
         '2017-11-19 12:43:30',
         {0: '2017-11-19 12:43:25', 4: '2017-11-19 12:43:29'}),
        ('* * 5,6,7 * 0,3 0',
         None,
         '2017-11-19 13:19:15',
         '2017-11-26 13:19:15',
         {0: '2017-11-19 14:00', 1: '2017-11-19 14:03', 2: '2017-11-19 15:00',
          19: '2017-11-19 23:03', 20: '2017-11-24 00:00', 21: '2017-11-24 00:03',
          22: '2017-11-24 01:00', 67: '2017-11-24 23:03', 68: '2017-11-25 00:00',
          115: '2017-11-25 23:03', 116: '2017-11-26 00:00', 143: '2017-11-26 13:03'}),
        ('* */10 1 0 0 0',
         None,
         '2017-01-01',
         '2018-01-01',
         {0: '2017-01-30', 1: '2017-02-20', 2: '2017-03-20', 3: '2017-04-10', 4: '2017-07-10',
          5: '2017-10-30', 6: '2017-11-20'}),
        ('* * * * * *',
         '2019-04-12 03:23:00',
         '2019-04-12 03:22',
         '2019-04-12 03:24',
         {0: '2019-04-12 03:23:01', 58: '2019-04-12 03:23:59'}),
    ]

    def to_string_provider(self) -> Tuple[str, str, datetime]:
        _time = '* * * * * * '
        for command in self.correct_commands:
            __timestamp_ = ' #' + str(int(parse('2017-11-16 23:59:59').timestamp()))
            for vv in (
                    ('',                      '2017-11-16 23:59:59', False, ' #2017-11-16 23:59:59'),
                    (' #123',                 '2017-11-16 23:59:59', False, ' #123'),
                    (' #2016-10-04 17:18:24', '2017-11-16 23:59:59', False, ' #2016-10-04 17:18:24'),
                    ('',                      '2017-11-16 23:59:59', True,  ' #2017-11-16 23:59:59'),
                    (' #123',                 '2017-11-16 23:59:59', True,  __timestamp_),
                    (' #2016-10-04 17:18:24', '2017-11-16 23:59:59', True,  ' #2017-11-16 23:59:59'),
            ):
                original_last_call = vv[0]
                date = parse(vv[1])
                execute = vv[2]
                expected = vv[3]
                yield (_time + command + original_last_call,
                       date,
                       execute,
                       _time + command + expected)

    @data_provider(correct_strings)
    def test_creating_from_correct_string(self, string: str) -> None:
        assert Task.from_string(string, Clock()) is not None

    @data_provider(empty_strings)
    def test_not_creating_from_empty_string(self, string: str) -> None:
        assert Task.from_string(string, Clock()) is None

    @data_provider(incorrect_strings)
    def test_throwing_exception_on_incorrect_string(self, string: str) -> None:
        with self.assertRaises(Exception):
            Task.from_string(string, Clock())

    @data_provider(skipped_provider)
    def test_skipped(self, task: str, now: str, expected_last_call: str) -> None:
        now = parse(now)
        expected_last_call = parse(expected_last_call)
        assert self\
            .__task(task, True, expected_last_call - timedelta(microseconds=1), now)\
            .skipped() is True
        assert self.__task(task, True, expected_last_call, now).skipped() is False

    @data_provider(calls_provider)
    def test_calls(self, task: str, last_call: str, _from: str, _to: str, expected_calls: dict) -> None:
        if not last_call:
            last_call = '2000-01-01 00:00:00'
        task = Task.from_string(task + ' ' + self.__command + '  #' + last_call, Clock())
        calls = task.calls(parse(_from), parse(_to))
        assert len(calls) == (max(expected_calls.keys()) + 1 if expected_calls else 0)
        for i, v in expected_calls.items():
            assert calls[i] == parse(v)

    @data_provider(to_string_provider)
    def test_converting_to_string(self, original: str, _datetime: datetime, execute: bool, expected: str) -> None:
        task = Task.from_string(original, MockClock(_datetime))
        if execute:
            task.execute()
        assert str(task) == expected

    def __task(self, time: str, command: bool = True, last_call: datetime = None, now: datetime = None):
        string = time
        if command:
            string += ' ' + self.__command
        if last_call:
            string += ' #' + str(int(last_call.timestamp()))
        return Task.from_string(string, MockClock(now) if now else Clock())
