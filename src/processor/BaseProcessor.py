import multiprocessing
import multiprocessing.queues
import logging

class BaseProcessor(object):
    def __init__(self, upstream, environment):
        self.environment = environment
        self.queue = upstream
        self._stopped = multiprocessing.Event()
        self.run_process = multiprocessing.Process(target=self._run)
        self.logr = logging.getLogger("BaseProcessor")

    def run(self):
        self.run_process.start()

    def _run(self):
        while not self._stopped.is_set():
            msg = self.next_message()
            if not msg:
                continue
            print(str(msg))

    def stop(self):
        self._stopped.set()

    def running(self):
        self.run_process.is_alive() and not self._stopped.is_set()

    def stopped(self):
        return self._stopped.is_set() and self.queue.qsize() == 0

    def next_message(self):
        ret_val = None
        if self.queue.qsize() > 0:
            try:
                ret_val = self.queue.get(block=False)
            except multiprocessing.queues.Empty:
                self.logr.error("Queue was empty when trying to get next message")
        return ret_val