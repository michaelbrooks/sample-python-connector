__author__ = "Nick Isaacs"
import time
from Queue import Queue
from src.stream.GnipJsonStreamClient import GnipJsonStreamClient
from src.utils.Envirionment import Envirionment

class GnipJsonStreamClientTest(object):
    def test_stream(self):
        downstream = Queue(5000)
        configuration = Envirionment()
        client = GnipJsonStreamClient(
            configuration.streamurl,
            configuration.streamname,
            configuration.username,
            configuration.password,
            configuration.filepath,
            configuration.rollduration,
            downstream,
            compressed=configuration.compressed
        )
        client.run()
        print("Waiting 5 seconds to fill up a queue with messages")
        time.sleep(5)
        assert downstream.qsize() > 0
