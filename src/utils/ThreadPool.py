import Worker
from Queue import Queue

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.tasks = Queue(self.num_threads)
        for _ in range(num_threads): Worker(self.tasks)
        self.running = False

    def add_task(self, func, *args, **kargs):
        """Add a task to the queue"""
        if not self.running:
            self.running = True
        self.tasks.put((func, args, kargs))

    def wait_completion(self):
        """Wait for completion of all the tasks in the queue"""
        self.tasks.join()
        self.running = False

    def is_running(self):
        """Check if boolean is running and there is something to run"""
        return self.running and self.tasks.qsize() > 0

    def force_kill(self):
        self.__init__(self, self.num_threads)
