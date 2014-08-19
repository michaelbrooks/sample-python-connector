#!/usr/bin/env python
__author__ = 'scott hendrickson'

import multiprocessing
import time
import gzip
import os
import logging

from src.processor.BaseProcessor import BaseProcessor
write_lock = multiprocessing.RLock()


class SaveThread(BaseProcessor):
    def __init__(self, _upstream, _feedname, _savepath):
        BaseProcessor.__init__(self, _upstream)
        self.savepath = _savepath
        self.feedName = _feedname
        self.timeStart = time.gmtime(time.time())

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
