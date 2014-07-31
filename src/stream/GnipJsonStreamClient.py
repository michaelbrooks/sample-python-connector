# This class wraps the GnpiRawStreamClient and parses the strings onto a
# FIFO queue
__author__ = "Nick Isaacs"

from src.stream.GnipRawStreamClient import GnipRawStreamClient
import thread
import json


class GnipJsonStreamClient(object):
    def __init__(self, _streamURL, _streamName, _userName, _password,
                 _filePath, _rollDuration, _producerQueue, compressed=True):
        self.gnip_raw_sream_client = GnipRawStreamClient(_streamURL, _streamName, _userName, _password,
                                                         _filePath, _rollDuration, True)
        self.producer_queue = _producerQueue

    def run(self):
        self.gnip_raw_sream_client.run()
        thread.start_new_thread(self.parse_string_buffer())

    def parse_string_buffer(self):
        for act in self.gnip_raw_sream_client.string_buffer().split("\n"):
            self.logger.debug(str(act))
            if act.strip() is None or act.strip() == '':
                continue
            self.producer_queue.put(json.loads(act))


