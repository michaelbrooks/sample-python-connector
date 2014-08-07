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

    def run(self):
        self.gnip_raw_sream_client.run()
        self.run_thread.start()
        print("Started application")
        # Block for stuff to appear
        while 0 >= self.producer_queue.qsize():
            pass

    def running(self):
        return (not self.stopped()) and 0 != self.producer_queue.qsize() or self.gnip_raw_sream_client.running()

    def stop(self):
        self._stop.set()
        self.gnip_raw_sream_client.stop()
        print("Cleanly stopped raw streaming client")

    def stopped(self):
        return self._stop.is_set()

    def queue(self):
        return self.producer_queue

    def parse_string_buffer(self):
        print("Starting to parse buffer")
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
            except Exception, e:
                self.logr.debug("There was an error: " + e.message)