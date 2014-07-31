__author__ =  "Nick Isaacs"

from src.utils import *

class BaseProcessor():
    def __init__(self, queue, poolSize, *args):
        self.queue = queue
        self.thread_pool = ThreadPool()

    def process(self, message):
        raise "Cannot run base processor"

    def next_message(self):
        self.queue.get()