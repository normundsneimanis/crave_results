import os
import threading
import pickle
import hyperopt
from CraveResults import CraveResultsException


class CraveResultsHyperopt:
    def __init__(self, logger):
        self.save_dir = "/var/lib/crave_results_db/hyperopt"
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

        self.hyperopt_data = {}
        self.save_file = os.path.join(self.save_dir, "hyperopts.pickle")
        if os.path.isfile(self.save_file):
            try:
                with open(self.save_file, "rb") as f:
                    self.hyperopt_data = pickle.loads(f.read())
                    self.logger.info("CraveResultsHyperopt() read data from file successfuly")
            except Exception as e:
                self.logger.error("CraveResultsHyperopt() failed reading saved file: %s" % str(e))
                self.hyperopt_data = {}
        self.lock = threading.Lock()
        self.updated = False

    def save_data(self):
        if not self.updated:
            return
        self.logger.debug("CraveResultsHyperopt() saving data")
        self.lock.acquire()
        with open(self.save_file, "wb") as f:
            f.write(pickle.dumps(self.hyperopt_data))
        self.updated = False
        self.lock.release()

    # LIST_HYPEROPT
    def list_hyperopt(self):
        self.lock.acquire()
        li = list(self.hyperopt_data.keys())
        self.lock.release()
        return li

    # REMOVE_HYPEROPT
    def remove_hyperopt(self, name) -> bool:
        removed = False
        self.lock.acquire()
        if name in self.hyperopt_data:
            del self.hyperopt_data[name]
            removed = True
            self.updated = True
            self.logger.info("CraveResultsHyperopt() removing %s" % name)
        self.lock.release()
        return removed

    # GET_HYPEROPT
    def get(self, name):
        if name in self.hyperopt_data:
            self.logger.info("CraveResultsHyperopt() lending %s" % name)
        else:
            self.logger.info("CraveResultsHyperopt() creating %s" % name)
            self.hyperopt_data[name] = hyperopt.Trials()
            self.updated = True

        return self.hyperopt_data[name]

    # UPDATE_HYPEROPT
    def update(self, name, new_data) -> bool:
        self.logger.info("CraveResultsHyperopt() updating %s" % name)
        if name not in self.hyperopt_data:
            self.logger.error("CraveResultsHyperopt() name %s not initialized" % name)
            return False
        self.hyperopt_data[name] = hyperopt.trials_from_docs(
            list(self.hyperopt_data[name]) + new_data)
        self.updated = True
        return True
