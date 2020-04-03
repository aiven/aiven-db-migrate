# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

import datetime
import random
import string
import threading
import time


def random_string(length=20):
    return "".join(random.choices(string.ascii_lowercase, k=length))


class Timeout(Exception):
    """Timeout"""


class TimerBase:
    def __init__(self):
        self._start = self.now()

    @staticmethod
    def now():
        return time.monotonic()

    def start_time(self):
        return self._start

    def reset(self):
        self._start = self.now()

    def elapsed(self):
        """Return seconds since starting timer"""
        return self.now() - self._start

    def elapsed_absolute(self):
        """Return timestamp for starting timer"""
        return datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=self.elapsed())


class Timer(TimerBase):
    """Multipurpose timer"""

    def __init__(self, *, timeout=None, sleep=1.0, what=None):
        super().__init__()
        self._what = what or "operation to complete"
        self._timeout = timeout
        self._sleep = sleep
        self._next_sleep_value = self._calculate_next_sleep_value()
        self._iters = 0
        self._last_sleep_start = self._start
        self._event = threading.Event()

    def get_timeout_value(self):
        return self._timeout

    def reset(self):
        super().reset()
        self._iters = 0

    def loop(self, *, raise_timeout=True, log=None):
        """Helper function to implement waiting loops like:
        timer = Timer(sleep=5, timeout=60)
        while timer.loop():
            if x:
                break
        which sleeps on every iteration after the first and raises an error on timeout
        """
        self._iters += 1
        if self._iters == 1:
            return True

        if self.timeout(raise_timeout=raise_timeout):
            return False  # timed out

        # Wait a bit and keep going
        if log:
            log.info("Waiting for %s, %.2fs elapsed", self._what, self.elapsed())
        self.sleep()
        return True

    def timeout(self, raise_timeout=False):
        """Return True if we are past the timeout moment"""
        if self._timeout is None:
            return False  # never timeout

        timeout_occurred = self.elapsed() >= self._timeout
        if raise_timeout and timeout_occurred:
            msg = "Timeout waiting for {} ({:.2f} seconds)".format(self._what, self._timeout)
            if isinstance(raise_timeout, Exception):
                raise Timeout(msg) from raise_timeout
            raise Timeout(msg)

        return timeout_occurred

    def time_to_timeout(self):
        """Return time until timer will timeout.

        <0 when timeout is already passed.
        Use .timeout() instead if you want to check whether timer has expired.
        """
        if self._timeout is None:
            # This is timer counting upwards, calling this method does not make much sense
            return None
        return self._timeout - self.elapsed()

    def next_sleep_length(self):
        """Return length of the next sleep in seconds"""
        sleep_time = self._next_sleep_value - min(self.now() - self._last_sleep_start, 0)
        if self._timeout is not None:
            # never sleep past timeout deadline
            sleep_time = min(sleep_time, (self.start_time() + self._timeout) - self.now())

        return max(sleep_time, 0.0)

    def interrupt(self):
        """Make a possible sleep() return immediately"""
        self._event.set()

    def is_interrupted(self):
        """Returns True if the timer has been interrupted and next call to sleep() will return immediately"""
        return self._event.is_set()

    def set_expired(self):
        """Set timer to timed out"""
        if self._timeout is not None:
            self._start = self.now() - self._timeout

    def sleep(self):
        """
        Sleep until next attempt should be performed or we are interrupted

        Attempt to synchronize exiting this method every 'sleep' interval,
        i.e. time spent outside this method is taken into account.
        """

        sleep_time = self.next_sleep_length()
        self._next_sleep_value = self._calculate_next_sleep_value()
        if sleep_time > 0.0:
            # only sleep if not long enough time was spent between iterations outside this function
            self._last_sleep_start = self.now()
            if self._event.wait(timeout=self.next_sleep_length()):
                self._event.clear()

    def _calculate_next_sleep_value(self):
        if not isinstance(self._sleep, tuple):
            return self._sleep
        return random.randrange(*self._sleep)
