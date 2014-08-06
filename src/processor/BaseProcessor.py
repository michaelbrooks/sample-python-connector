__author__ = "Nick Isaacs"

DEFAULT_THREADS_POOL_SIZE = 8


def next_message(self):
    self.queue.get()


def stop(self):
    self.stopped().set()
    self.thread_pool.wait()


def stopped(self):
    return self._stopped


def is_stopped(self):
    self.stopped().is_set()