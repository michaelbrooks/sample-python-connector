__author__ = "Nick Isaacs"
from Queue import Queue
from src.stream.GnipJsonStreamClient import GnipJsonStreamClient
from src.utils.Envirionment import Envirionment


class TestHelper(object):
    def __init__(self):
        self.config = Envirionment()

    def client(self):
        return GnipJsonStreamClient(
            self.config.streamurl,
            self.config.streamname,
            self.config.username,
            self.config.password,
            self.config.filepath,
            self.config.rollduration,
            compressed = self.config.compressed
        )