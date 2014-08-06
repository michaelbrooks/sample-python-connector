#!/usr/bin/env python
__author__ = 'scott hendrickson, nick isaacs'
import time
import urllib2
import httplib
import ssl
import base64
import zlib
import socket
import logging
import os
from multiprocessing import Event, Process, Manager
from ctypes import c_char_p
import threading

MAX_QUEUE_SIZE = 5000
CHUNK_SIZE = 2 ** 17  # decrease for v. low volume streams, > max record size
GNIP_KEEP_ALIVE = 30  # 30 sec gnip timeout
MAX_BUF_SIZE = 2 ** 22  # bytes records to hold in memory
MAX_ROLL_SIZE = 2 ** 30  # force time-period to roll forward
DELAY_FACTOR = 1.5  # grow by DELAY_FACTOR - 1 % with each failed connection
DELAY_MAX = 150  # maximum delay in seconds
DELAY_MIN = 0.1  # minimum delay in seconds
DELAY_RESET = 60 * 10  # Connected for the long, then reset the delay to min
NEW_LINE = '\r\n'


class GnipRawStreamClient(object):
    def __init__(self, _streamURL, _streamName, _userName, _password,
                 _filePath, _rollDuration, compressed=True):
        self.logr = logging.getLogger("GnipRawStreamClient")
        self.logr.info('GnipStreamClient started')
        self.compressed = compressed
        self.logr.info('Stream compressed: %s' % str(self.compressed))
        self.rollDuration = _rollDuration
        self.streamName = _streamName
        self.streamURL = _streamURL
        self.filePath = _filePath
        self.headers = {'Accept': 'application/json',
                        'Connection': 'Keep-Alive',
                        'Accept-Encoding': 'gzip',
                        'Authorization': 'Basic %s' % base64.encodestring(
                            '%s:%s' % (_userName, _password))
        }
        self._stop = Event()
        self.manager = Manager()
        self.string_buffer = self.manager.Value(c_char_p, "")
        delay_reset = time.time()
        delay = DELAY_MIN
        self.run_process = Process(target=self._run, args=(delay, delay_reset))
        self.time_roll_start = time.time()

    def _run(self, delay, delay_reset):
        while not self.stopped():
            try:
                self.get_stream()
                self.logr.error("Forced disconnect")
                delay = DELAY_MIN
            except ssl.SSLError, e:
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.logr.error("Connection failed: %s (delay %2.1f s)" % (e, delay))
            except httplib.IncompleteRead, e:
                self.logr.error("Streaming chunked-read error (data chunk lost): %s" % e)

            except urllib2.HTTPError, e:
                self.logr.error("HTTP error: %s" % e)

            except urllib2.URLError, e:
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.logr.error("URL error: %s (delay %2.1f s)" % (e, delay))
            except socket.error, e:
                # Likely reset by peer (why?)
                delay = delay * DELAY_FACTOR if delay < DELAY_MAX else DELAY_MAX
                self.logr.error("Socket error: %s (delay %2.1f s)" % (e, delay))
            if time.time() - delay_reset > DELAY_RESET:
                # if we have been connected for a long time before this error,
                # then reset the delay
                delay = DELAY_MIN
            delay_reset = time.time()
            time.sleep(delay)

    def run(self):
        self.run_process.start()

    def stop(self):
        lock = threading.RLock()
        lock.acquire()
        with lock:
            self._stop.set()
        self.run_process.join()

    def get_stream(self):
        self.logr.info("Connecting")
        req = urllib2.Request(self.streamURL, headers=self.headers)
        response = urllib2.urlopen(req, timeout=(1 + GNIP_KEEP_ALIVE))
        # sometimes there is a delay closing the connection, can go directly to the socket to control this
        realsock = response.fp._sock.fp._sock
        try:
            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            self.buffer_string("")
            roll_size = 0
            while not self.stopped():
                if self.compressed:
                    chunk = decompressor.decompress(response.read(CHUNK_SIZE))
                else:
                    chunk = response.read(CHUNK_SIZE)
                if chunk == '':
                    return
                self.buffer_string(chunk)
                test_time = time.time()
                test_roll_size = roll_size + len(self.get_string_buffer())
                if self.trigger_process(test_time, test_roll_size):
                    if test_roll_size == 0:
                        self.logr.info("No data collected this period (testTime=%s)" % test_time)
                    self.get_string_buffer().replace("}{", "}%s{" % NEW_LINE)
                    [records, tmp_buffer] = self.get_string_buffer().rsplit(NEW_LINE, 1)
                    self.set_string_buffer(tmp_buffer)
                    timeSpan = test_time - self.time_roll_start
                    # self.logr.debug("recsize=%d, %s, %s, ts=%d, dur=%d" %
                    #                 (len(records), self.streamName, self.filePath,
                    #                  test_time, timeSpan))
                    if self.roll_forward(test_time, test_roll_size):
                        self.time_roll_start = test_time
                        roll_size = 0
                    else:
                        roll_size += len(records)
        except None, e:
            self.logr.error("Buffer processing error (%s) - restarting connection" % e)
            realsock.close()
            response.close()
            raise e

    def roll_forward(self, ttime, tsize):
        # these trigger both processing and roll forward
        if ttime - self.time_roll_start >= self.rollDuration:
            # self.logr.debug("Roll: duration (%d>=%d)" %
            #                 (ttime - self.time_roll_start, self.rollDuration))
            return True
        if tsize >= MAX_ROLL_SIZE:
            # self.logr.debug("Roll: size (%d>=%d)" %
            #                 (tsize, MAX_ROLL_SIZE))
            return True
        return False

    def trigger_process(self, ttime, tsize):
        if NEW_LINE not in self.get_string_buffer():
            return False
        if len(self.get_string_buffer()) > MAX_BUF_SIZE:
            # self.logr.debug("Trigger: buffer size (%d>%d)" %
            #                 (len(self.string_buffer), MAX_BUF_SIZE))
            return True
        return self.roll_forward(ttime, tsize)

    def set_string_buffer(self, str):
        self.string_buffer.value = str

    def get_string_buffer(self):
        return self.string_buffer.value

    def buffer_string(self, str):
        self.string_buffer.value = self.get_string_buffer() + str

    def stopped(self):
        lock = threading.RLock()
        lock.acquire()
        with lock:
            status = self._stop.is_set()
            return status

    def info(self, title):
        print(title)
        print('module name:', __name__)
        if hasattr(os, 'getppid'):  # only available on Unix
            print('parent process:', os.getppid())
        print('process id:', os.getpid())