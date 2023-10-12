"""
You can run test.py Test.test_simple_run multiple times with disabled removal of table.
"""
import os
import random
import shutil
import time
import unittest
from dotenv import load_dotenv
from crave_results import CraveResults, CraveResultsException
import string
import multiprocessing
import hyperopt
import numpy as np


class Test(unittest.TestCase):
    def setUp(self):
        save_dir = os.path.expanduser("~/.crave_results/save")
        if os.path.isdir(save_dir):
            for f in os.listdir(save_dir):
                os.unlink(os.path.join(save_dir, f))
        self.db = CraveResults()
        if os.path.isdir("tmp"):
            shutil.rmtree("tmp")
        os.mkdir("tmp")
        self.db.remove_experiment("detect_pedestrians")

    def tearDown(self):
        self.db.remove_experiment("detect_pedestrians")
        self.db = None
        shutil.rmtree("tmp")

    def test_init_exception(self):
        config = dict(
            learning_rate=0.01,
            momentum=0.2,
            architecture="CNN",
            dataset_id="peds-0192",
            infra="AWS",
        )

        with self.assertRaises(CraveResultsException) as context:
            self.db.init({
                "notes": "tweak baseline",
                "tags": ["baseline", "paper1"],
                "config": config
            })

    # Disabled as it does not output error messages to console but to log file instead
    # import sys
    # from io import StringIO
    # from unittest.mock import patch
    # def test_encrypt_failed(self):
    #     db = CraveResultsTestUnencrypted()
    #     config = dict(
    #         learning_rate=0.01,
    #         momentum=0.2,
    #         architecture="CNN",
    #         dataset_id="peds-0192",
    #         infra="AWS",
    #     )
    #
    #     # send unencrypted packet to server and see if unencrypted error is returned and handled correctly
    #     with patch('sys.stderr', new=StringIO()) as fakeOutput:
    #         db.init({
    #             "project": "detect_pedestrians",
    #             "notes": "tweak baseline",
    #             "tags": ["baseline", "paper1"],
    #             "config": config
    #         })
    #         time.sleep(2.1)
    #         self.assertTrue("Unable to decrypt" in fakeOutput.getvalue().strip())

    def test_server_unavailable1(self):
        # TODO Create results that are saved locally. Enable server afterward and check that they are sent
        pass

    def test_server_unavailable2(self):
        # TODO Results saved by part 1 are sent and checked here
        pass

    def test_simple_run(self):
        tables_start = self.db.get_experiments()

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

        # Due to async nature of communication, we must wait for results to be inserted in server database
        print("Waiting for data to be written in database")
        time.sleep(65.)
        tables = self.db.get_experiments()
        self.assertListEqual(sorted(tables), sorted(tables_start+['detect_pedestrians']), "Table created")

        runs = self.db.get_runs('detect_pedestrians', 'notes')
        run_times = [i[1] for i in runs]
        this_run = runs[run_times.index(self.db.run_time)]
        self.assertTrue(self.db.run_time in run_times)
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
        with open("tmp/0.jpeg", "rb") as f:
            image = f.read()
        self.assertTrue(image == image0)

    def test_get_artifact(self):
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

        with open(os.path.join("tmp/test_weights.pt"), "wb") as f:
            f.write(os.urandom(random.randint(1000, int(1e6))))
        self.db.log_artifact({
            "weights": self.db.binary("tmp/test_weights.pt")
        })

        # Due to async nature of communication, we must wait for results to be inserted in server database
        time.sleep(2.1)

        runs = self.db.get_runs('detect_pedestrians', 'notes')
        run_times = [i[1] for i in runs]
        this_run = runs[run_times.index(self.db.run_time)]

        with open("tmp/test_weights.pt", "rb") as f:
            file1 = f.read()

        file2 = self.db.get_artifact('detect_pedestrians', "weights", this_run[0])

        self.assertEqual(file2, file1)

    def test_parallel_hyperopt(self):
        def asd2(i):
            result_logger = CraveResults()
            trials = result_logger.get_hyperopt("testwork%d" % i)

            space = {
                "learning_rate": hyperopt.hp.uniform("learning_rate", -2.5, -.5)
            }

            def optimizable_f(learning_rate):
                return random.uniform(0., 100.)

            treshs = hyperopt.fmin(
                optimizable_f,
                space,
                trials=trials,
                algo=hyperopt.tpe.suggest,
                show_progressbar=False,
                max_evals=len(trials) + 1)

            self.assertTrue(result_logger.put_hyperopt("testwork%d" % i, list(trials)[-1:]))

        processes = []
        for i in range(50):
            proc = multiprocessing.Process(target=asd2, args=([i]))
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()

        result_logger = CraveResults()
        for i in range(50):
            self.assertTrue(result_logger.remove_hyperopt("testwork%d" % i))

    def test_parallel_insertion(self):
        def asd():
            result_logger = CraveResults()

            experiment_config = dict(
                learning_rate=0.01,
                momentum=0.2,
                architecture="CNN",
                dataset_id="peds-0192",
                infra="AWS",
            )

            result_logger.init({
                "experiment": "detect_pedestrians",
                'config': experiment_config
            })

            result_logger.log({"test": 1.234, "test23": ["abc", "123", 456]})

            with open("tmp/file_name.asd", "wb") as f:
                f.write(os.urandom(random.randint(int(1e4), int(1e7))))

            result_logger.log({"inference image": result_logger.binary("tmp/file_name.asd")})

            file_bytes = os.urandom(random.randint(int(1e4), int(1e7)))
            result_logger.log({"bytes image": result_logger.binary(file_bytes, "bytes_file.asd")})

            file_string = ''.join(random.choice(string.printable) for i in
                                  range(random.randint(int(1e4), int(1e7)))).encode()
            result_logger.log({"log string": result_logger.binary(file_string, "log_file.txt")})

            file_string = ''.join(random.choice(string.printable) for i in
                                  range(random.randint(int(1e4), int(1e7)))).encode()
            result_logger.log({"log string": result_logger.binary(file_string, "log_file.txt")})

        processes = []
        for i in range(10):
            proc = multiprocessing.Process(target=asd)
            proc.start()
            processes.append(proc)

        for proc in processes:
            proc.join()

    def test_nan(self):
        def asd():
            result_logger = CraveResults()

            experiment_config = dict(
                learning_rate=0.01,
                momentum=0.2,
                architecture="CNN",
                dataset_id="peds-0192",
                infra="AWS",
            )

            result_logger.init({
                "experiment": "detect_pedestrians",
                'config': experiment_config
            })

            with self.assertRaises(CraveResultsException) as context:
                result_logger.log({"test": np.nan, "test23": ["abc", "123", 456]})


if __name__ == '__main__':
    load_dotenv()
    unittest.main()
