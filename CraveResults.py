"""
Takes results from script, packs them and sends over network to the server.
 * If server is unavailable, saves them on disk and sends later.
 * Send results at most once per second.
"""
import struct
import time
import pickle
import traceback
from .CraveResultsLogType import CraveResultsLogType
from .CraveResultsCommand import CraveResultsCommand
from .CraveBase import _get_payload, CraveCrypt, CraveCryptTest
from .GzipRotator import GZipRotator, GzipRotatingFileHandler
import threading
import socket
import errno
import fcntl
import os
import atexit
import logging
import logging.handlers
import posix_ipc
import hashlib


class CraveResultsException(Exception):
    pass


class CraveResults:
    def __init__(self, active=True):
        self.table_name = None
        self.run_time = 0.0
        self.active = active
        self.initialized = False
        self.last_send_time = 0.0
        self.log_data = []
        self.sender = None
        self.log_lock = threading.Lock()
        self._send_delay = 1.
        self.connect_timeout = 5.
        self.host, self.port = os.getenv('CRAVE_RESULTS_HOST'), int(os.getenv('CRAVE_RESULTS_PORT', 65099))
        if not self.host:
            raise ValueError("CraveResults requires CRAVE_RESULTS_HOST to be configured")
        self.name = None
        self.experiment = None
        self.finish_thread = False
        self.save_dir = os.path.expanduser("~/.crave_results/save")
        if not os.path.isdir(self.save_dir):
            os.makedirs(self.save_dir)
        self.logs_dir = os.path.expanduser("~/.crave_results/logs")
        if not os.path.isdir(self.logs_dir):
            os.makedirs(self.logs_dir)
        self.logger = logging.getLogger("CraveResults")
        console_handler = logging.StreamHandler()
        log_formatter = logging.Formatter("CraveResults: %(message)s")
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.INFO)
        self.logger.addHandler(console_handler)
        self.crypt = CraveCrypt(self.logger)
        self.semaphore_uploader_name = "crave_results_file_uploader"
        self.shared_status_socket = None

        if not posix_ipc.SEMAPHORE_TIMEOUT_SUPPORTED:
            raise CraveResultsException("CraveResults requires OS with sem_timedwait() support")
        try:
            sem = posix_ipc.Semaphore(self.semaphore_uploader_name)
            sem.acquire(5.)
            sem.release()
        except posix_ipc.ExistentialError:
            posix_ipc.Semaphore(self.semaphore_uploader_name, posix_ipc.O_CREAT, initial_value=1)
        except posix_ipc.BusyError:
            self.logger.debug("Semaphore busy more than 5 seconds, killed process? Re-creating")
            sem = posix_ipc.Semaphore(self.semaphore_uploader_name)
            sem.unlink()
            posix_ipc.Semaphore(self.semaphore_uploader_name, posix_ipc.O_CREAT, initial_value=1)
        self.semaphore_uploader = posix_ipc.Semaphore(self.semaphore_uploader_name)

        atexit.register(self.finish)

    def finish(self):
        self.logger.debug("Running finish()")
        if self.sender:
            self.logger.debug("Joining send thread")
            self.finish_thread = True
            while self.sender.is_alive():
                time.sleep(0.001)
            self.sender.join(0.)
            self.sender = None
            self.logger.debug("Send thread joined")

    def init(self, data: dict) -> None:
        if not self.active:
            return
        # contains results db info and optional config
        if 'project' not in data and 'experiment' not in data:
            raise CraveResultsException("experiment(project) field not given")

        if 'run_time' not in data:
            data['run_time'] = time.time()
        self.run_time = data['run_time']

        if 'experiment' in data:
            self.name = data['experiment'] + '-' + str(data['run_time'])
            self.experiment = data['experiment']
            del data['experiment']
        elif 'project' in data:
            self.name = data['project'] + '-' + str(data['run_time'])
            self.experiment = data['project']
            del data['project']

        log_file_name = os.path.join(self.logs_dir, "%s.log" % self.name)
        logging_level = logging.DEBUG
        self.logger = logging.getLogger("CraveResults")
        self.crypt.logger = self.logger
        self.logger.setLevel(logging_level)
        log_filehandler = GzipRotatingFileHandler(filename=log_file_name, maxBytes=1024 * 1024 * 5, backupCount=10)
        log_filehandler.rotator = GZipRotator()
        log_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        log_filehandler.setFormatter(log_formatter)
        self.logger.addHandler(log_filehandler)

        if True:
            console_handler = logging.StreamHandler()
            log_formatter = logging.Formatter("CraveResults: %(message)s")
            console_handler.setFormatter(log_formatter)
            console_handler.setLevel(logging.WARNING)
            self.logger.addHandler(console_handler)

        config = None
        if 'config' in data:
            config = data['config']
            del data['config']
        self._append_info(data)
        self.log_data.append(struct.pack("!b", CraveResultsLogType.INIT) + pickle.dumps(data))
        self.last_send_time = time.time()
        self.sender = threading.Thread(target=self.send_thread, args=())
        self.sender.start()

        if config:
            self._append_info(config)
            self._append_log(struct.pack("!b", CraveResultsLogType.HYPERPARAMS) + pickle.dumps(config))

        self.initialized = True

    def _restart_thread(self):
        if not self.sender.is_alive():
            self.sender.join()
            self.sender = threading.Thread(target=self.send_thread, args=())
            self.sender.start()

    def _save_failed_packet(self, packet):
        save_file_id = 0
        save_file_name = "%s-%010d.packet" % (self.name, save_file_id)
        while os.path.isfile(os.path.join(self.save_dir, save_file_name)):
            save_file_id += 1
            save_file_name = "%s-%010d.packet" % (self.name, save_file_id)

        with open(os.path.join(self.save_dir, save_file_name), "wb") as f:
            f.write(packet)

    def send_thread(self):
        self.logger.debug("Starting send_thread")
        time.sleep(1.)
        self.logger.debug("Running send_thread")
        try:
            saved_files = os.listdir(self.save_dir)
            if len(saved_files):
                self.logger.debug("Found %d saved files. Sending" % len(saved_files))
                self.semaphore_uploader.acquire(0.001)
                for save_file_name in saved_files:
                    save_file_full = os.path.join(self.save_dir, save_file_name)
                    with open(save_file_full, "rb") as f:
                        whole_packet = f.read()
                    sock = None
                    try:
                        sock = self._create_socket()
                        sock.sendall(whole_packet)

                        sock.settimeout(max(30., (len(whole_packet)/1024/1024)))
                        received = sock.recv(65535)
                        if self._process_answer(whole_packet, received, sock, save=False):
                            os.unlink(save_file_full)
                    except socket.timeout:
                        self.logger.debug("Timeout waiting for answer from server when re-uploading results")
                        break
                    except Exception as e:
                        self.logger.debug("Failed re-uploading results: %s" % str(e))
                        break
                    finally:
                        if sock:
                            sock.close()

                self.semaphore_uploader.release()
        except posix_ipc.BusyError:
            self.logger.debug("File upload semaphore busy")

        self.logger.debug("Finished send_thread")

        if ((len(self.log_data) and time.time() > self.last_send_time + self._send_delay)
                or (self.finish_thread and len(self.log_data))):
            # Send log_data to server
            self.log_lock.acquire()
            log = self.log_data
            self.log_data = []
            self.log_lock.release()

            packet = b''
            for portion in log:
                portion_len = len(portion)
                packet += struct.pack("!L", portion_len) + portion
                self.logger.debug("Adding portion of size %d to packet" % portion_len)

            packet = self.crypt.encrypt(packet)
            packet = struct.pack("!bL", CraveResultsCommand.UPLOAD_RESULTS, len(packet)) + packet
            self.logger.debug("Prepared packet of length %d bytes." % len(packet))

            sock = None
            try:
                sock = self._create_socket()
                sock.sendall(packet)

                sock.settimeout(30.0)
                received = sock.recv(65535)
                self._process_answer(packet, received, sock)

            except socket.timeout:
                self.logger.debug("Timeout waiting for answer from server")
                # self.logger.error(''.join(traceback.format_stack()))
                self._save_failed_packet(packet)
            except Exception as e:
                self.logger.debug("Failed uploading results: %s" % str(e))
                self._save_failed_packet(packet)
            finally:
                if sock:
                    sock.close()

    def _process_answer(self, packet, received, sock, save=True) -> bool:
        if len(received) >= 1:
            result = struct.unpack("!b", received[0:1])[0]
            if result == CraveResultsCommand.COMMAND_OK:
                self.logger.debug("Success")
                return True
            elif result == CraveResultsCommand.COMMAND_FAILED_ENCRYPTED:
                payload = _get_payload(self.logger, sock, (self.host, self.port), received[1:])
                if payload is None:
                    raise CraveResultsException("Failed to get all packet data from server while processing "
                                                "COMMAND_FAILED_ENCRYPTED")
                message = self.crypt.decrypt(payload)
                self.logger.debug("CraveResults failed to upload results. Server said (enc): %s" %
                                  message.decode())
                if save:
                    self._save_failed_packet(packet)
            elif result == CraveResultsCommand.COMMAND_FAILED:
                payload = _get_payload(self.logger, sock, (self.host, self.port), received[1:])
                if payload is None:
                    raise CraveResultsException("Failed to get all packet data from server while processing "
                                                "COMMAND_FAILED")
                self.logger.debug("CraveResults failed to upload results. Server said: %s" % payload.decode())
                if save:
                    self._save_failed_packet(packet)
            else:
                self.logger.debug("CraveResults failed to upload results. Unknown response %s" % str(result))
                if save:
                    self._save_failed_packet(packet)
            self.last_send_time = time.time()
        else:
            self.logger.debug("Server closed socket without giving answer.")
            if save:
                self._save_failed_packet(packet)

    def _append_log(self, log):
        self.log_lock.acquire()
        self.log_data.append(log)
        self.log_lock.release()

    def _append_info(self, data):
        data['__experiment'] = self.experiment
        data['__run_time'] = self.run_time

    def config(self, data: dict) -> None:
        if not self.active or not self.initialized:
            return
        # Initialize run hyperparams.
        self._append_log(struct.pack("!b", CraveResultsLogType.HYPERPARAMS) + pickle.dumps(data))
        self._restart_thread()

    def log(self, data: dict) -> None:
        if not self.active or not self.initialized:
            return
        self._append_info(data)
        # Appends a new step to the 'history' object and updates the 'summary' object
        self._append_log(struct.pack("!b", CraveResultsLogType.LOG) + pickle.dumps(data))
        self._restart_thread()

    def log_history(self, data: dict) -> None:
        if not self.active or not self.initialized:
            return
        self._append_info(data)
        # Appends a new step to the 'history' object without updating the 'summary' object
        self._append_log(struct.pack("!b", CraveResultsLogType.LOG_HISTORY) + pickle.dumps(data))
        self._restart_thread()

    def log_summary(self, data: dict):
        if not self.active or not self.initialized:
            return
        self._append_info(data)
        self._append_log(struct.pack("!b", CraveResultsLogType.LOG_SUMMARY) + pickle.dumps(data))
        self._restart_thread()

    def log_artifact(self, data: dict) -> None:
        if not self.active or not self.initialized:
            return
        self._append_info(data)
        # Log run artifacts, such as log files, images or video
        self._append_log(struct.pack("!b", CraveResultsLogType.LOG_ARTIFACT) + pickle.dumps(data))
        self._restart_thread()

    def binary(self, f, name=""):
        """
        Accepts file path, string or bytes

        :param f: File path, string or bytes.
        :param name: File name. Required if file is given as string or bytes.
        :return: File representation to be saved to database.
        """

        if isinstance(f, str):
            if os.path.isfile(f):
                with open(f, "rb") as fi:
                    file_data = fi.read()
                name = os.path.basename(f)
                object_type = "bytes"
            else:
                raise CraveResultsException("File name required when passing as string or bytes.")
        elif isinstance(f, bytes):
            file_data = f
            if not name:
                raise CraveResultsException("File name required when passing as string or bytes.")
            object_type = "bytes"
        else:
            raise CraveResultsException("Invalid file type given to CraveResults.binary(): %s. "
                                        "Expecting file path, string or bytes" % str(type(f)))
        blake = hashlib.blake2b()
        blake.update(file_data)
        checksum = blake.hexdigest()

        data = {
            "checksum": checksum,
        }
        self._append_info(data)
        data_encoded = pickle.dumps(data)
        self._append_log(struct.pack("!bLH", CraveResultsLogType.BINARY, len(file_data), len(data_encoded))
                         + file_data + data_encoded)
        return {
            "name": name,
            "checksum": checksum,
            "object_type": object_type
        }

    def _create_socket(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.connect_timeout)
            sock.connect((self.host, self.port))
            sock.settimeout(None)
            fcntl.fcntl(sock, fcntl.F_SETFL)
        except Exception as e:
            self.logger.debug(e)
            raise CraveResultsException("Failed to open connection") from e

        return sock

    def _get_answer(self, request):
        sock = self._create_socket()
        sock.sendall(request)

        sock.settimeout(5.0)
        try:
            received = sock.recv(65535)
        except socket.timeout:
            self.logger.debug("Timeout waiting for answer.")
            return None

        answer = struct.unpack("!b", received[0:1])[0]
        if answer == CraveResultsCommand.COMMAND_OK:
            payload = _get_payload(self.logger, sock, (self.host, self.port), received[1:])
            if payload is None:
                self.logger.debug("Failed to get all packet data")
                return None
            sock.close()
            payload = self.crypt.decrypt(payload)
            return payload
        elif answer in [CraveResultsCommand.COMMAND_FAILED, CraveResultsCommand.COMMAND_FAILED_ENCRYPTED]:
            error_msg_len = struct.unpack("!L", received[1:5])[0]
            if len(received[3:]) < error_msg_len:
                self.logger.debug("Server returned error but error message is too short (%d/%d)" %
                                  (len(received[3:]), error_msg_len))
                return None
            if answer == CraveResultsCommand.COMMAND_FAILED:
                error_msg = received[5:].decode()
            else:
                error_msg = self.crypt.decrypt(received[5:])
            self.logger.debug("Server returned message: %s" % error_msg)
            return None
        else:
            self.logger.debug("Unknown answer given from server: %d" % answer)
            return None

    def list_hyperopt(self):
        request = struct.pack("!b", CraveResultsCommand.LIST_HYPEROPT)
        payload = self._get_answer(request)
        if not payload:
            return None
        return pickle.loads(payload)

    def remove_hyperopt(self, name):
        request_data = self.crypt.encrypt(pickle.dumps(name))
        request = struct.pack("!bL", CraveResultsCommand.REMOVE_HYPEROPT, len(request_data)) + request_data

        payload = self._get_answer(request)
        if not payload:
            return None
        return True

    def get_hyperopt(self, name):
        request_data = self.crypt.encrypt(pickle.dumps(name))
        request = struct.pack("!bL", CraveResultsCommand.GET_HYPEROPT, len(request_data)) + request_data

        payload = self._get_answer(request)
        if not payload:
            return None
        return pickle.loads(payload)

    def put_hyperopt(self, name, trial):
        request_data = self.crypt.encrypt(pickle.dumps([name, trial]))
        request = struct.pack("!bL", CraveResultsCommand.PUT_HYPEROPT, len(request_data)) + request_data
        payload = self._get_answer(request)
        if not payload:
            return None
        return payload

    def _request(self, request_data):
        sock = self._create_socket()
        sock.sendall(request_data)

        sock.settimeout(5.0)
        try:
            received = sock.recv(65535)
        except socket.timeout:
            self.logger.debug("Timeout waiting for answer from server.")
            # self.logger.error(''.join(traceback.format_stack()))
            return None, None

        answer = struct.unpack("!b", received[0:1])[0]
        payload_length = struct.unpack("!L", received[1:5])[0]
        new_data_len = len(received[5:])
        self.logger.debug("_get_payload() got request for %d bytes with %d bytes already" %
                          (payload_length, new_data_len))
        new_data_ = [received[5:]]

        if payload_length == new_data_len:
            sock.close()
            return answer, b''.join(new_data_)

        data = None
        try:
            while True:
                received = sock.recv(65535)
                if len(received) == 0:
                    self.logger.debug("Client closed connection.")
                    break
                new_data_.append(received)
                new_data_len += len(received)
                if new_data_len == payload_length:
                    data = b''.join(new_data_)
                    break
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                self.logger.debug('No data available')
                if new_data_len == payload_length:
                    data = b''.join(new_data_)
            else:
                self.logger.debug("Socket error: %s" % str(e))
        except Exception as e:
            self.logger.debug("Another exception: %s" % str(e))
        finally:
            sock.close()

        return answer, data

    def _handle_failure(self, answer, data):
        if answer == CraveResultsCommand.COMMAND_FAILED:
            print("Error: Server returned: %s" % data.decode())
        elif answer == CraveResultsCommand.COMMAND_FAILED_ENCRYPTED:
            message = self.crypt.decrypt(data)
            print("Error: Server returned: %s" % message.decode())
        else:
            if answer:
                print("Error: Server returned unknown response: %d" % answer)

    def get_experiments(self) -> list:
        request = struct.pack("!b", CraveResultsCommand.LIST_EXPERIMENTS) + \
                  self.crypt.encrypt(pickle.dumps({"dummy": str(time.time()).encode()}))
        answer, data = self._request(request)
        if answer == CraveResultsCommand.LIST_EXPERIMENTS and data:
            data = self.crypt.decrypt(data)
            experiments = pickle.loads(data)
            return experiments
        else:
            self._handle_failure(answer, data)
            return []

    def get_runs(self, experiment: str, run_identified_by: str = "") -> list:
        request = {
            "experiment": experiment,
            "run_identified_by": run_identified_by
        }
        return self._handle_request_return_list(CraveResultsCommand.GET_RUNS, request)

    def get_fields(self, experiment: str) -> dict:
        request = {
            "experiment": experiment
        }
        request_data = self.crypt.encrypt(pickle.dumps(request))
        request = struct.pack("!b", CraveResultsCommand.GET_FIELDS) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.GET_FIELDS and data:
            data = self.crypt.decrypt(data)
            fields = pickle.loads(data)
            return fields
        else:
            self._handle_failure(answer, data)
            return {}

    def get_rows(self, experiment: str, rows: dict, special=False):
        request = {
            "experiment": experiment,
            "rows": rows,
            "special": special
        }
        request_data = self.crypt.encrypt(pickle.dumps(request))
        request = struct.pack("!b", CraveResultsCommand.GET_ROW) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.GET_ROW and data:
            data = self.crypt.decrypt(data)
            fields = pickle.loads(data)
            return fields
        else:
            self._handle_failure(answer, data)
            return None

    def get_field(self, experiment: str, field: str, run_id: int) -> list:
        request = {
            "experiment": experiment,
            "field": field,
            "run_id": run_id
        }
        return self._handle_request_return_list(CraveResultsCommand.GET_FIELD, request)

    def get_artifact(self, experiment: str, field: str, run_id: int) -> bytes:
        request = {
            "experiment": experiment,
            "field": field,
            "run_id": run_id
        }

        request = self.crypt.encrypt(pickle.dumps(request))
        request = struct.pack("!bL", CraveResultsCommand.GET_ARTIFACT, len(request)) + request
        answer, data = self._request(request)
        if answer == CraveResultsCommand.COMMAND_OK and data:
            data = self.crypt.decrypt(data)
            return data
        else:
            self._handle_failure(answer, data)
            raise CraveResultsException("Failed getting file from server")

    def get_history(self, experiment: str, field: str, run_id: int) -> list:
        request = {
            "experiment": experiment,
            "field": field,
            "run_id": run_id
        }
        return self._handle_request_return_list(CraveResultsCommand.GET_HISTORY_BY_ID, request)

    def get_history_(self, experiment: str, field: str, run_time: float) -> list:
        request = {
            "experiment": experiment,
            "field": field,
            "run_time": run_time
        }
        return self._handle_request_return_list(CraveResultsCommand.GET_HISTORY_BY_TIME, request)

    def get_summary(self, experiment: str, field: str, run_id: int) -> list:
        request = {
            "experiment": experiment,
            "field": field,
            "run_id": run_id
        }
        return self._handle_request_return_list(CraveResultsCommand.GET_SUMMARY, request)

    def get_file(self, checksum):
        if 'checksum' in 'md5':
            request = {
                "checksum": checksum['checksum'],
            }
        else:
            request = {
                "checksum": checksum,
            }
        request_data = self.crypt.encrypt(pickle.dumps(request))
        request = struct.pack("!bL", CraveResultsCommand.GET_FILE, len(request_data)) + request_data

        answer = self._get_answer(request)
        if answer:
            return answer
        raise CraveResultsException("Failed getting answer from server")

    def _handle_request_return_list(self, command, request) -> list:
        request_data = self.crypt.encrypt(pickle.dumps(request))
        request = struct.pack("!b", command) + request_data
        answer, data = self._request(request)
        if answer == command and data:
            data = self.crypt.decrypt(data)
            field = pickle.loads(data)
            return field
        else:
            self._handle_failure(answer, data)
            return []

    def remove_experiment(self, experiment: str) -> bool:
        request_data = self.crypt.encrypt(pickle.dumps(experiment))
        request = struct.pack("!bL", CraveResultsCommand.REMOVE_EXPERIMENT, len(request_data)) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.COMMAND_OK:
            # data = self.crypt.decrypt(data)
            return True
        else:
            self._handle_failure(answer, data)
            return False

    def shared_status_create(self, name: str, value):
        request_data = self.crypt.encrypt(pickle.dumps([name, value]))
        request = struct.pack("!bL", CraveResultsCommand.CREATE_SHARED_STATUS, len(request_data)) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.COMMAND_OK:
            return True
        else:
            self._handle_failure(answer, data)
            return False

    def shared_status_get(self, name: str):
        request_data = self.crypt.encrypt(pickle.dumps(name))
        request = struct.pack("!bL", CraveResultsCommand.UPDATE_SHARED_STATUS, len(request_data)) + request_data
        sock = self._create_socket()
        sock.sendall(request)

        sock.settimeout(10.0)
        try:
            received = sock.recv(65535)
        except socket.timeout:
            self.logger.debug("Timeout waiting for answer from server.")
            return None

        request_type = struct.unpack("!b", received[0:1])[0]
        if request_type == CraveResultsCommand.COMMAND_OK:
            payload = _get_payload(self.logger, sock, (self.host, self.port), received[1:])
            if payload is None:
                self.logger.debug("Failed to get all packet data")
                return None

            payload = self.crypt.decrypt(payload)
            value = pickle.loads(payload)
            self.shared_status_socket = sock
            return value
        else:
            self._handle_failure(request_type, received[1:])
            return None

    def shared_status_put(self, name: str, value) -> bool:
        if not self.shared_status_socket:
            raise CraveResultsException("shared_status_get() must be called before shared_status_put()")
        if self.shared_status_socket._closed:
            raise ValueError("Connection is closed, please call shared_status_put() immediately after "
                             "shared_status_get()")
        sock = self.shared_status_socket
        request_data = self.crypt.encrypt(pickle.dumps([name, value]))
        request = struct.pack("!L", len(request_data)) + request_data
        sock.sendall(request)
        sock.settimeout(10.0)
        try:
            received = sock.recv(65535)
        except socket.timeout:
            self.logger.debug("Timeout waiting for answer from server.")
            return False

        result = struct.unpack("!b", received[0:1])[0]
        if result == CraveResultsCommand.COMMAND_OK:
            payload = _get_payload(self.logger, sock, (self.host, self.port), received[1:])
            if payload is None:
                self.logger.debug("Failed to get all packet data")
                return False
            return True
        else:
            return False

    def shared_status_list(self) -> list:
        request_data = self.crypt.encrypt(pickle.dumps(True))
        request = struct.pack("!bL", CraveResultsCommand.LIST_SHARED_STATUS, len(request_data)) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.COMMAND_OK:
            data = self.crypt.decrypt(data)
            return pickle.loads(data)
        else:
            self._handle_failure(answer, data)
            return []

    def shared_status_remove(self, name) -> bool:
        request_data = self.crypt.encrypt(pickle.dumps(name))
        request = struct.pack("!bL", CraveResultsCommand.REMOVE_SHARED_STATUS, len(request_data)) + request_data
        answer, data = self._request(request)
        if answer == CraveResultsCommand.COMMAND_OK:
            return True
        else:
            self._handle_failure(answer, data)
            return False


class CraveResultsTestUnencrypted(CraveResults):
    def __init__(self):
        super().__init__()
        self.crypt = CraveCryptTest(self.logger)
