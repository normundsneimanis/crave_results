import mmap
import io
import posix_ipc
import pickle
import time


class SharedStatus:
    def __init__(self, name="python_shared_status"):
        """
        Shared status between different processes.

        :param name: Unique name in running system
        """
        self.name = name
        self.semaphore_name = "/%s" % name
        self._lock = None

    def initialize(self, initial_value) -> None:
        """
        Initialize semaphore and shared memory segment with initial value.

        :param initial_value: Initial value to insert in shared memory segment
        :return: None
        """
        try:
            with posix_ipc.Semaphore(self.semaphore_name) as s:
                pass
        except posix_ipc.ExistentialError:
            posix_ipc.Semaphore(self.semaphore_name, posix_ipc.O_CREAT, initial_value=1)
        msg = self._create_message(initial_value)

        mem = posix_ipc.SharedMemory(self.semaphore_name, posix_ipc.O_CREAT, size=msg.getbuffer().nbytes)
        mapfile = mmap.mmap(mem.fd, mem.size)
        mapfile.write(msg.getbuffer())
        mapfile.close()
        mem.close_fd()

    def is_initialized(self) -> bool:
        try:
            with posix_ipc.Semaphore(self.semaphore_name) as s:
                return True
        except posix_ipc.ExistentialError:
            return False

    @staticmethod
    def _create_message(data):
        msg = io.BytesIO()
        pickle.dump(data, msg)
        msg.seek(0)
        return msg

    def get(self):
        """
        Returns current status

        :return: Current status
        """
        with posix_ipc.Semaphore(self.semaphore_name) as s:
            return self.get_locked()

    def lock(self) -> None:
        """
        Locks shared memory segment

        :return: None
        """

        if not self._lock:
            self._lock = posix_ipc.Semaphore(self.semaphore_name)
        self._lock.acquire()

    def unlock(self) -> None:
        """
        Releases shared memory segment

        :return: None
        """

        self._lock.release()
        self._lock = None

    def get_locked(self):
        """
        Gets shared memory contents. Lock must be previously acquired with lock()

        :return:
        """

        try:
            mem = posix_ipc.SharedMemory("/%s" % self.name, 0)
        except posix_ipc.ExistentialError:
            return None
        with mmap.mmap(mem.fd, mem.size) as mm:
            data = pickle.load(mm)
            mem.close_fd()
            return data

    def put(self, value) -> None:
        """
        Locks shared memory segment and updates current value

        :param value: Value to be shared between processes
        :return: None
        """
        with posix_ipc.Semaphore(self.semaphore_name) as s:
            return self.put_locked(value)

    def put_locked(self, value) -> None:
        """
        Gets shared memory contents. Lock must be previously acquired with lock()

        :param value: Data to insert in shared memory segment
        :return: None
        """
        try:
            mem = posix_ipc.SharedMemory("/%s" % self.name, 0)
            mem.close_fd()
            mem.unlink()
        except posix_ipc.ExistentialError:
            pass

        msg = self._create_message(value)
        mem = posix_ipc.SharedMemory("/%s" % self.name, posix_ipc.O_CREAT, size=msg.getbuffer().nbytes)
        mapfile = mmap.mmap(mem.fd, mem.size)
        mapfile.write(msg.getbuffer())
        mapfile.close()
        mem.close_fd()
        return

    def remove(self) -> None:
        """
        Removes shared memory segment and cleans up

        :return: None
        """
        sem = posix_ipc.Semaphore(self.semaphore_name)
        sem.acquire()
        sem.release()
        sem.unlink()
        try:
            mem = posix_ipc.SharedMemory("/%s" % self.name, 0)
            mem.close_fd()
            mem.unlink()
        except posix_ipc.ExistentialError:
            pass


def test_writer(queue):
    i = 0
    while True:
        # Safely updates value in shared memory
        with posix_ipc.Semaphore(queue.semaphore_name) as s:
            queue.put_locked(queue.get_locked() + 1)
        i += 1


def test_reader(queue):
    start = time.time()
    num = 0
    while True:
        # Reads value from shared memory
        ret = queue.get()
        num += 1
        if start + 2. < time.time():
            start = time.time()
            print("#writes: %d #reads: %d" % (num, ret))


if __name__ == '__main__':
    import multiprocessing

    q = SharedStatus("shared_status")
    q.initialize(0)

    writer = multiprocessing.Process(target=test_writer, args=[q])
    writer2 = multiprocessing.Process(target=test_writer, args=[q])
    reader = multiprocessing.Process(target=test_reader, args=[q])
    reader2 = multiprocessing.Process(target=test_reader, args=[q])
    writer.start()
    writer2.start()
    reader.start()
    reader2.start()
