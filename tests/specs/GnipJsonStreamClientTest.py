__author__ = "Nick Isaacs"
import time
from tests.TestHelper import TestHelper


class GnipJsonStreamClientTest(object):

    def test_stream(self):
        test_helper = TestHelper()
        client = test_helper.client
        client.run()
        while not client.running():
            time.sleep(1)
        print("Waiting 5 seconds to fill up a queue with messages")
        time.sleep(5)
        client.stop()
        if client.queue().qsize() > 0:
            pass

    def test_stop_stream(self):
        test_helper = TestHelper()
        client = test_helper.client
        client.run()
        while not client.running():
            time.sleep(1)
        print("Waiting 5 seconds to fill up a queue with messages")
        time.sleep(5)
        client.stop()
        queue_size = client.queue().qsize()
        print("Sleep to prove queue has stopped, additional messages should not be streamed")
        time.sleep(1)
        if client.queue().qsize() == queue_size:
            pass