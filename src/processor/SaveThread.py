#!/usr/bin/env python
__author__ = 'scott hendrickson'

from threading import Event
from threading import RLock
from threading import Thread
import time
import gzip
import os
import logging
import threadpool

write_lock = RLock()


class SaveThread(Thread):
    def __init__(self, _upstream, _pool_size, _feedname, _savepath):
        self.queue = _upstream
        self.pool_size = _pool_size
        self.logr = logging.getLogger("SaveThread")
        self.savepath = _savepath
        self.feedName = _feedname
        self.timeStart = time.gmtime(time.time())
        self._stopped = Event()

    def run(self):
        try:
            self.logr.debug("started")
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
            print("Stopped: " + str(self.is_stopped()))
            while not self.is_stopped():
                chunk = self.next_message()
                if None != chunk:
                    with write_lock:
                        request = threadpool.makeRequests(self.write, file_name, chunk)
                        self.thread_pool.putRequest(request)
                        # self.thread_pool.add_task(self.write, file_name, chunk)
        except Exception, e:
            self.logr.error("saveAs failed, exiting thread (%s). Exiting." % e)
            raise e

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
        self.queue.get()

    def stop(self):
        self.stopped().set()
        self.thread_pool.wait()

    def stopped(self):
        return self._stopped

    def is_stopped(self):
        self.stopped().is_set()