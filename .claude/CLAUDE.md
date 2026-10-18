# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Cronus is a custom cron daemon in one file, `cronus.py`. Its main feature: it runs "skipped" jobs. A skipped job is a job whose time came while the machine was off. Cronus runs such a job as soon as the service starts again.

Run it with a crontab file as the only argument:

```
python cronus.py /path/to/crontab
```

## Crontab format

Each line is `mon dom dow h m s command`. The six time fields accept `*`, `*/N`, `N`, `N-M`, and comma lists. In `dow`, 7 and 0 both mean Sunday. A line that starts with `#` is a comment.

Cronus writes back to the crontab file. It appends ` #<last call>` to each task line. The last call is a Unix timestamp or `YYYY-MM-DD HH:MM:SS`, and Cronus keeps the format that the line already has. This suffix is the persistent state that lets Cronus detect skipped jobs after a reboot.

## Tests

The tests use `unittest`. Run them from the `tests/` directory, with the repository root on `PYTHONPATH`. The tests open `__sandbox/...` with relative paths, and they import `cronus` and `unittest_data_provider` from the root.

```
cd tests
PYTHONPATH=.. python -m unittest test_task                     # fast unit tests for Task
PYTHONPATH=.. python -m unittest test_task.TestTask.<method>   # one test
PYTHONPATH=.. python -m unittest test_cronus                   # slow functional test
```

`test_cronus` runs the real daemon in a thread. A `MockClock` reads the fake current time from `tests/__sandbox/time.txt`. Tasks in `__sandbox/crontab_begin` append to `__sandbox/sandbox`, and the test compares that output with the expected events. The test edits `crontab_begin` during the run and restores it at the end. At the end it compares the result with `crontab_end`. The test uses fixed `time.sleep` delays for each OS, so it is timing-sensitive.

`unittest_data_provider/` is a vendored copy of a small data-provider decorator for tests.

## Architecture

- `Task` parses one crontab line with the regex at the top of the file. It expands each field to a sorted list of values. `calls(from, to)` lists the run times in a window. `__expected_last_call()` searches backward for the most recent scheduled time. `is_skipped()` compares that time with the stored last call. `execute()` starts the command with `Popen(shell=True)` and does not start it again while the previous process is alive. `__del__` terminates the process tree with `psutil`.
- `Cronus.run()` is the main loop. It reads the file and runs skipped tasks. Then it builds a one-day queue of `Event`s (a time plus the tasks due at that time), sleeps until each event, and executes its tasks.
- Control flow uses exceptions. `FileChangedException` comes from the file watcher thread. `WakeUpException` comes when the clock jumps more than 10 minutes past the expected time, for example after sleep or suspend. Both restart the loop and cause a full re-read.
- On re-read, Cronus maps tasks by line index. If a line is unchanged, it keeps the old `Task` object, so the running process and the last call survive. Otherwise it creates a new `Task`.
- Cronus writes last calls back to the file at most every 5 seconds (the checkpoint), and also in `__del__`. After its own write, it drains the watcher queue so that the write does not count as a user change.
- The loop calls `gc.collect()` and clears references in `finally`. This lets the `__del__` of old tasks run under PyPy, which has no reference counting. Keep this behavior when you change the loop.
- All time comes from an injected `Clock`. Do not call `datetime.now()` directly, because the tests mock time through `Clock`.
