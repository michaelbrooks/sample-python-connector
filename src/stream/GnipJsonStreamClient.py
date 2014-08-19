# This class wraps the GnpiRawStreamClient and parses the strings onto a
# FIFO queue
__author__ = "Nick Isaacs"

import json
import logging
import multiprocessing
from src.stream.GnipRawStreamClient import GnipRawStreamClient


class GnipJsonStreamClient(object):
    def __init__(self, _streamURL, _streamName, _userName, _password,
                 _filePath, _rollDuration, compressed=True):
        self.gnip_raw_sream_client = GnipRawStreamClient(_streamURL, _streamName, _userName, _password,
                                                         _filePath, _rollDuration, compressed)
        self.producer_queue = multiprocessing.Queue()
        self.logr = logging.getLogger("GnipJsonStreamClient")
        self._stop = multiprocessing.Event()
        self.run_thread = multiprocessing.Process(target=self.parse_string_buffer)
        self._started = False

    def started(self):
        return self._started

    def run(self):
        self.gnip_raw_sream_client.run()
        self.run_thread.start()
        self.logr.debug("Started application")

    def running(self):
        return not self.stopped()

    def stop(self):
        self._stop.set()
        self.gnip_raw_sream_client.stop()
        self.logr.debug("Cleanly stopped raw streaming client")

    def stopped(self):
        return self._stop.is_set()

    def queue(self):
        return self.producer_queue

    def parse_string_buffer(self):
        self.logr.debug("Starting to parse buffer")
        while not self.stopped():
            if not isinstance(self.gnip_raw_sream_client.get_string_buffer(), basestring):
                continue
            try:
                chunks = self.gnip_raw_sream_client.get_string_buffer().split("\n")
                for chunk in chunks:
                    if chunk.strip() is None or chunk.strip() == '':
                        continue
                    the_hash = json.loads(chunk)
                    self.producer_queue.put(the_hash)
                    if not self._started:
                        self._started = True
            except ValueError:
                self.logr.error("There was a ValueError in the chunk: " + chunk)
            except Exception, e:
                self.logr.error("There was an error: " + e.message)
                raise e
