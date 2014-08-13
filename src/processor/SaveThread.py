#!/usr/bin/env python
__author__ = 'scott hendrickson'

import multiprocessing.queues
import multiprocessing
import time
import gzip
import os
import logging

write_lock = multiprocessing.RLock()


class SaveThread(object):
    def __init__(self, _upstream, _pool_size, _feedname, _savepath):
        self.queue = _upstream
        self.pool_size = _pool_size
        self.logr = logging.getLogger("SaveThread")
        self.savepath = _savepath
        self.feedName = _feedname
        self.timeStart = time.gmtime(time.time())
        self._stopped = multiprocessing.Event()
        self.run_process = multiprocessing.Process(target=self._run)

    def run(self):
        self.run_process.start()

    def _run(self):
        self.logr.debug("started")
        file_name = self.setup_out_file()
        self.run_loop(file_name)

    def run_loop(self, file_name):
        while not self._stopped.is_set():
            chunk = self.next_message()
            if None != chunk:
                with write_lock:
                    self.write(file_name, str(chunk))

    def write(self, file_name, string):
        try:
            fp = gzip.open(file_name, "a")
            fp.write(string)
            fp.close()
            self.logr.info("saved file %s" % file_name)
        except Exception, e:
            self.logr.error("write failed: %s" % e)
            raise e

    def next_message(self):
        ret_val = None
        if self.queue.qsize() > 0:
            try:
                ret_val = self.queue.get(block=False)
            except multiprocessing.queues.Empty:
                self.logr.error("Queue was empty when trying to get next message")
        return ret_val

    def stop(self):
        self._stopped.set()

    def stopped(self):
        return self._stopped.is_set() and self.queue.qsize() == 0

    def running(self):
        self.run_process.is_alive() and not self._stopped.is_set()

    def setup_out_file(self):
        file_path = "/".join([
            self.savepath,
            "%d" % self.timeStart.tm_year,
            "%02d" % self.timeStart.tm_mon,
            "%02d" % self.timeStart.tm_mday,
            "%02d" % self.timeStart.tm_hour])
        try:
            os.makedirs(file_path)
            self.logr.info("directory created (%s)" % file_path)
        except OSError:
            self.logr.info("directory exists (%s)" % file_path)
        name = self.feedName + "_"
        name += "-".join([
            "%d" % self.timeStart.tm_year,
            "%02d" % self.timeStart.tm_mon,
            "%02d" % self.timeStart.tm_mday])
        name += "_%02d%02d" % (self.timeStart.tm_hour, self.timeStart.tm_min)
        name += ".gz"
        file_name = file_path + "/" + name
        return file_name
