import unittest.mock
# from freezegun import freeze_time
from datetime import datetime, timedelta
# from dateutil.parser import parse
import threading
import time
import os
from typing import List

from cronus import Cronus, Clock


sandbox_dir = os.path.abspath('__sandbox')
time_file = sandbox_dir + '/time.txt'
crontab = sandbox_dir + '/crontab_begin'
sandbox = sandbox_dir + '/sandbox'
time_format = '%Y-%m-%d %H:%M:%S'
time_format_full = time_format + '.%f'
stop = False


class MockClock(Clock):
    def time(self):
        while True:
            global stop, time_file
            if stop:
                raise SystemExit
            with open(time_file, 'r') as file:  # todo: something smarter
                _time = file.read()
                if _time:
                    return datetime.strptime(_time, time_format_full)


# class StoppableThread(threading.Thread):
#     """Thread class with a stop() method. The thread itself has to check
#     regularly for the stopped() condition."""
#
#     def __init__(self, *args, **kwargs):
#         super(StoppableThread, self).__init__(*args, **kwargs)
#         # self._stop_event = threading.Event()
#         self.__pid = None
#         # print(self.__pid)
#
#     def start(self):
#         print('start..')
#         super(StoppableThread, self).start()
#         self.__pid = os.getpid()
#         print(40, self.__pid)
#
#     def stop(self):
#         subprocess.call('kill -9 ' + str(self.__pid), shell=True)
#         # self._stop_event.set()
#
#     # def stopped(self):
#     #     return self._stop_event.is_set()


class TestCronus(unittest.TestCase):
    """
    Functional test, takes some time to run
    """

    def test(self):
        self.__clear_sandbox()
        initial_crontab = self.__read_lines(crontab)
        try:
            self.__set_time(self.__parse_time('2017-11-18 13:33:02'))
            self.__run_daemon_thread()
            self.__assert_events({
                '2017-11-18 13:33:03': (['22:15_2'], []),
                '2017-11-18 13:33:05': ([], []),
                '2017-11-18 13:33:10': ([], []),
                '2017-11-18 13:33:15': ([], ['15s']),
                '2017-11-18 13:33:29': ([], []),
                '2017-11-18 13:33:30': ([], ['15s']),
                '2017-11-18 13:33:31': ([], []),
                '2017-11-18 13:33:46': (['15s'], []),
                '2017-11-18 13:44:45': (['15s'], ['15s']),
                '2017-11-18 13:45:00': ([], ['15m', '15s']),
                '2017-11-18 13:45:15': ([], ['15s']),
                # sleep
                '2017-11-18 22:14:45': (['15m', '15s'], ['15s']),
                '2017-11-18 22:15:00': ([], ['15m', '22:15_1', '22:15_2', '15s']),
                '2017-11-18 22:15:15': ([], ['15s']),
            })
            self.__set_time(self.__parse_time('2017-11-18 22:15:16'))
            self.__change_file([2], ['* * *  *    *  */5  echo 5s       >> __sandbox/sandbox'])
            self.__assert_events({
                '2017-11-18 22:15:20': ([], ['5s']),
                '2017-11-18 22:15:25': ([], ['5s']),
                '2017-11-18 22:15:30': ([], ['15s', '5s']),
                '2017-11-18 22:15:36': (['5s'], []),
                # sleep
                '2017-11-19 22:14:44': (['15m', '15s', '23:59:59', '5s'], []),
                '2017-11-19 22:14:45': ([], ['15s', '5s']),
                '2017-11-19 22:14:50': ([], ['5s']),
                '2017-11-19 22:14:55': ([], ['5s']),
                '2017-11-19 22:15:00': ([], ['15m', '22:15_2', '15s', '5s']),
                '2017-11-19 22:15:05': ([], ['5s']),
                # reboot
                '2017-11-19 23:15:06': ('reboot', ['15m', '15s', '5s']),
                '2017-11-19 23:15:10': ([], ['5s']),
                '2017-11-19 23:15:15': ([], ['15s', '5s']),
                # reboot
                '2018-04-09 17:48:06': ('reboot', ['15m', '22:15_2', '15s', '23:59:59', '5s']),
                # reboot
                '2018-04-09 17:48:07': ('reboot', []),
                # sleep
                '2018-04-09 22:00:00': (['15m', '15s', '5s'], ['15m', '15s', '5s']),
                '2018-04-09 23:59:58': (['15m', '22:15_2', '15s', '5s'], []),
                '2018-04-09 23:59:59': ([], ['23:59:59']),
                '2018-04-10 00:00:00': ([], ['15m', '15s', '5s']),
                '2018-04-10 00:00:01': ([], []),
                '2018-04-10 22:00:00': (['15m', '15s', '5s'], ['15m', '15s', '5s']),
            })

            lines = self.__read_lines(crontab)
            lines[2] = lines[2].replace('22   15', '21   45')
            self.__write(crontab, lines)

            self.__assert_events({
                '2018-04-10 22:00:01': (['22:15_2'], []),
            })

            self.__set_time(self.__parse_time('2020-03-07 22:50:30'))
            self.__change_file([], ['* * *  *    *    0  echo 1m       >> __sandbox/sandbox'])
            self.__assert_events({
                '2021-03-07 22:50:30': ('reboot', ['15m', '22:15_2', '15s', '23:59:59', '5s']),
                '2021-03-07 22:55:30': ('reboot', ['15s', '5s', '1m']),
            })

            self.__stop()
            assert self.__read_lines(crontab) == self.__read_lines(sandbox_dir + '/crontab_end')
        finally:
            self.__stop()
            self.__write(crontab, initial_crontab)

    # @unittest.mock.patch('datetime.datetime.now', new_callable=_time)
    # @unittest.mock.patch('cronus.time_', side_effect=time2)
    def __run_daemon_thread(self,
                            # cronus_time: unittest.mock.patch,
                            ) -> threading.Thread:
        # cronus_time
        # time_()
        # cronus_time.assert_called_once_with()
        # unittest.mock.MagicMock()
        # with freeze_time(lambda: self.__time()):  # todo: replace with self.__time ?
        thread = threading.Thread(target=self.__run_daemon)
        thread.daemon = True
        thread.start()
        return thread

    @staticmethod
    def __run_daemon() -> None:
        global stop
        stop = False
        try:
            Cronus(crontab, MockClock(), 0.01).run()
        except SystemExit:
            pass

    def __assert_events(self, expected_events: dict) -> None:
        for _time, expected_events in sorted(expected_events.items(), key=lambda x: x[0]):
            _time = self.__parse_time(_time)
            if expected_events[0] == 'reboot':
                self.__stop()
                self.__run_daemon_thread()
            else:
                self.__set_time(_time - timedelta(milliseconds=1))
                prev = self.__read_sandbox()
                print('prev events', _time - timedelta(milliseconds=1), prev, expected_events)
                assert prev == self.__normalize_events(expected_events[0])
            self.__set_time(_time)
            sandbox_final = self.__read_sandbox()
            print('now events', _time, sandbox_final, expected_events)
            assert sandbox_final == self.__normalize_events(expected_events[1])

    def __change_file(self, lines_to_remove: list, lines_to_add: list) -> None:
        new_crontab = self.__read_lines(crontab)
        for line in lines_to_remove:
            del new_crontab[line]
        for line in lines_to_add:
            new_crontab.append(line)
        for i in range(len(new_crontab)):
            if new_crontab[i][-1:] != '\n':
                new_crontab[i] += '\n'
        self.__write(crontab, new_crontab)

    @staticmethod
    def __read_lines(filename: str) -> List[str]:
        with open(filename, 'r') as file:
            return file.readlines()

    @staticmethod
    def __write(filename: str, content) -> None:
        with open(filename, 'w') as file:
            if isinstance(content, str):
                file.write(content)
            elif isinstance(content, list):
                file.writelines(content)
            else:
                raise Exception

    def __set_time(self, _datetime: datetime) -> None:
        self.__write(time_file, _datetime.strftime(time_format_full))

    @staticmethod
    def __parse_time(_time: str) -> datetime:
        return datetime.strptime(_time, time_format)

    def __clear_sandbox(self) -> None:
        self.__write(sandbox, '')

    def __read_sandbox(self) -> List[str]:
        self.__let_daemon_work(False)
        res = self.__read_lines(sandbox)
        self.__clear_sandbox()
        return res

    @staticmethod
    def __let_daemon_work(expect_stop: bool) -> None:
        # approximate (heuristic) minimal required delays
        sleep = 0.2
        if expect_stop:
            sleep = 0.5  # needed for graceful stop of child processes
        time.sleep(sleep)

    def __stop(self) -> None:
        global stop
        stop = True
        self.__let_daemon_work(True)

    @staticmethod
    def __normalize_events(events: List[str]) -> List[str]:
        return [event + '\n' for event in events]
