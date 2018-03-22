# cronus

```
A custom crontab. Main feature: executing "skipped" jobs, i.e. job, whose execution time
has come when PC was off.
F.e., if a task should have been executed on 10:00, but from 9:00 to 11:00 machine was off,
then this task will be immediately executed as soon as machine will turn on (and launch the
cronus service).
```
Syntaxis for jobs:

`mon dom dow h m s command`
