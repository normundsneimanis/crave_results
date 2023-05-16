"""
You can run test.py Test.test_simple_run multiple times with disabled removal of table.
"""
import os
import pickle
import random
import shutil
import unittest
from dotenv import load_dotenv
from CraveResults import CraveResults


class Test(unittest.TestCase):
    def setUp(self):
        save_dir = os.path.expanduser("~/.crave_results/save")
        if os.path.isdir(save_dir):
            shutil.rmtree(save_dir)
        self.db = CraveResults()
        if os.path.isdir("tmp"):
            shutil.rmtree("tmp")
        os.mkdir("tmp")

    def tearDown(self):
        self.db = None

    def _run_test(self):
        config = dict(
            learning_rate=0.01,
            momentum=0.2,
            architecture="CNN",
            dataset_id="peds-0192",
            infra="AWS",
        )

        self.db.init({
            "project": "detect_pedestrians",
            "notes": "tweak baseline",
            "tags": ["baseline", "paper1"],
            "config": config
        })

        max_iters = 10
        losses = []
        accs = []
        for it in range(max_iters):
            loss = 1 - it * 1.0 / max_iters
            acc = it * 1.0 / max_iters
            with open(os.path.join("tmp/%d.jpeg" % it), "wb") as f:
                f.write(os.urandom(random.randint(1000, int(1e6))))

            self.db.log(
                {
                    "loss": loss,
                    "acc": acc,
                    "some_text": "Some text %d" % it,
                    "augmentation image": self.db.binary("tmp/%d.jpeg" % it, "%d.jpg" % it)
                }
            )
            losses.append(loss)
            accs.append(acc)

        with open(os.path.join("tmp/test2.jpeg"), "wb") as f:
            f.write(os.urandom(random.randint(1000, int(1e6))))
        self.db.log_artifact({
            "confusion matrix": self.db.binary("tmp/test2.jpeg")
        })

    def test_server_unavailable1(self):
        self._run_test()
        self.assertTrue(True)
        with open("tmp/reentrant", "wb") as f:
            f.write(pickle.dumps(self.db.run_time))
        shutil.copyfile("tmp/0.jpeg", "tmp/reentrant.jpeg")
        print("Now enable server and run test_unavailable2.py with %s" % str())


if __name__ == '__main__':
    load_dotenv()
    unittest.main()
