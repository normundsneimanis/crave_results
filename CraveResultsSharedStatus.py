import os
import threading
import pickle
from CraveBase import CraveResultsException


class CraveResultsSharedStatus:
    def __init__(self, logger):
        self.save_dir = "/var/lib/crave_results_db/shared_status"
        self.logger = logger
        if not self.save_dir:
            raise CraveResultsException("Save dir %s must exist" % self.save_dir)

        try:
            testfile_name = os.path.join(self.save_dir, "testfile")
            with open(testfile_name, "w") as f:
                f.write("asd")
            os.unlink(testfile_name)
        except PermissionError:
            raise CraveResultsException("Save dir %s must be writable" % self.save_dir)

        self.status_data = {}
        self.save_file = os.path.join(self.save_dir, "status.pickle")
        if os.path.isfile(self.save_file):
            try:
                with open(self.save_file, "rb") as f:
                    self.status_data = pickle.loads(f.read())
                    self.logger.info("CraveResultsSharedStatus() read data from file successfully")
            except Exception as e:
                self.logger.error("CraveResultsSharedStatus() failed reading saved file: %s" % str(e))
                self.status_data = {}
        self.lock = threading.Lock()
        self.updated = threading.Event()

    def save_data(self):
        if not self.updated.is_set():
            return
        self.logger.debug("CraveResultsSharedStatus() saving data")
        self.lock.acquire()
        with open(self.save_file, "wb") as f:
            f.write(pickle.dumps(self.status_data))
        self.updated.clear()
        self.lock.release()

    def list(self):
        self.lock.acquire()
        li = list(self.status_data.keys())
        self.lock.release()
        return li

    def remove(self, name) -> bool:
        removed = False
        self.lock.acquire()
        if name in self.status_data:
            del self.status_data[name]
            removed = True
            self.updated.set()
            self.logger.info("CraveResultsSharedStatus() removing %s" % name)
        self.lock.release()
        return removed

    def create(self, name, value):
        self.logger.info("CraveResultsSharedStatus() creating %s" % name)
        self.status_data[name] = value
        self.updated.set()

    def get(self, name):
        if name in self.status_data:
            self.logger.info("CraveResultsSharedStatus() lending %s" % name)
        else:
            return None

        return self.status_data[name]

    def update(self, name, new_data) -> bool:
        self.logger.info("CraveResultsSharedStatus() updating %s" % name)
        if name not in self.status_data:
            self.logger.error("CraveResultsSharedStatus() name %s not initialized" % name)
            return False
        self.status_data[name] = new_data
        self.updated.set()
        return True
