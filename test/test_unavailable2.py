"""
You can run test.py Test.test_simple_run multiple times with disabled removal of table.
"""
import os
import pickle
import random
import shutil
import time
import unittest
from dotenv import load_dotenv
from CraveResults import CraveResults


class Test(unittest.TestCase):
    def setUp(self):
        self.db = CraveResults()

    def tearDown(self):
        self.db.remove_experiment("detect_pedestrians")
        self.db = None
        shutil.rmtree("tmp")

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

    def test_server_unavailable2(self):
        self._run_test()

        time.sleep(5.1)
        with open("tmp/reentrant", "rb") as f:
            expected_run_time = pickle.loads(f.read())

        tables = self.db.get_experiments()
        self.assertListEqual(tables, ['detect_pedestrians'], "Table created")

        runs = self.db.get_runs('detect_pedestrians', 'notes')
        run_times = [i[1] for i in runs]
        self.assertTrue(expected_run_time in run_times)
        this_run = runs[run_times.index(expected_run_time)]
        self.assertEqual(this_run[2], 'tweak baseline')
        run_params = self.db.get_fields('detect_pedestrians')
        self.assertDictEqual(run_params, {
            'run': ['notes', 'tags', 'run_time'],
            'config': ['learning_rate', 'momentum', 'architecture', 'dataset_id', 'infra'],
            'summary': ['loss', 'acc', 'some_text', 'augmentation_image'],
            'history': ['loss', 'acc', 'some_text', 'augmentation_image'],
            'file': ['confusion_matrix'],
            'zfile': ['checksum'],
            'gpu': [], 'vmstat': []
        })
        losses = self.db.get_history('detect_pedestrians', "loss", this_run[0])
        self.assertListEqual(losses, [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.30000000000000004, 0.19999999999999996,
                                      0.09999999999999998])
        accs = self.db.get_history('detect_pedestrians', "acc", this_run[0])
        self.assertListEqual(accs, [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
        text_data = self.db.get_history('detect_pedestrians', "some_text", this_run[0])
        self.assertEqual(text_data[0], 'Some text 0')
        self.assertEqual(text_data[1], 'Some text 1')
        self.assertEqual(text_data[9], 'Some text 9')
        history_images = self.db.get_history('detect_pedestrians', "augmentation_image", this_run[0])
        self.assertTrue(history_images[0]['name'] == "0.jpeg")

        image0 = self.db.get_file(history_images[0]['checksum'])
        with open("tmp/reentrant.jpeg", "rb") as f:
            image = f.read()
        self.assertTrue(image == image0)


if __name__ == '__main__':
    load_dotenv()
    unittest.main()
