from __future__ import annotations
from typing import IO, Optional
import abc
import fcntl
import os

from my import AbstractMethodException


class InstanceLock(abc.ABC):
    """One instance per crontab file. Other crontabs may have their own instances.
    Held while Cronus runs: `__enter__` fails if another instance holds it"""

    @abc.abstractmethod
    def __enter__(self) -> InstanceLock:
        raise AbstractMethodException()

    @abc.abstractmethod
    def __exit__(self, *_: object) -> None:
        raise AbstractMethodException()


class FlockLock(InstanceLock):
    """`flock` on `<real path of crontab>.lock`: not the crontab itself,
    because it is replaced on each write, and the lock would stay on the old inode"""

    def __init__(self, filename: str) -> None:
        self.__filename: str = filename
        self.__path: str = os.path.realpath(filename) + '.lock'
        self.__file: Optional[IO] = None

    def __enter__(self) -> FlockLock:
        while True:
            lock_file = open(self.__path, 'w')
            try:
                fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                lock_file.close()
                raise Exception(f"Already running for {self.__filename}")
            if self.__is_at(lock_file):
                break
            lock_file.close()  # the previous owner deleted it after we opened it: we locked a deleted file
        self.__file = lock_file
        return self

    def __exit__(self, *_: object) -> None:
        if lock_file := self.__file:
            self.__file = None
            if self.__is_at(lock_file):  # not deleted and recreated by someone else
                os.unlink(self.__path)  # while still locked, so that the next owner never locks a deleted file
            lock_file.close()  # releases the lock

    def __is_at(self, file: IO) -> bool:
        try:
            return os.path.samestat(os.fstat(file.fileno()), os.stat(self.__path))
        except FileNotFoundError:
            return False
