__author__ = "Nick Isaacs"
import shutil
import time
import os
import tests.TestHelper
from src.processor.MongoProcessor import MongoProcessor

from src.utils.Envirionment import Envirionment


class SaveThreadProcessorTest(object):
    def setup(self):
        self.test_helper = tests.TestHelper.TestHelper()
        self.client = self.test_helper.client
        this_path = os.path.dirname(__file__)
        save_path = os.path.join(this_path, '../out')
        if os.path.exists(save_path):
            shutil.rmtree(save_path)
        os.mkdir(save_path)
        self.save_path = save_path

    def test_processor(self):
        self.client.run()
        print "Running Mongo stream test"
        processor = MongoProcessor(self.client.queue(), Envirionment())
        processor.run()
        print("Sleep 5 seconds to allow thread to process messages")
        run_until = time.time() + 15
        while time.time() < run_until:
            pass
        processor.stop()
        self.client.stop()
        while processor.running() or self.client.running():
            pass
