import copy
import time
import hashlib
import pickle
import shutil
import socket
import sys
import os
import signal
import threading
import logging.handlers
from dotenv import load_dotenv
from CraveBase import _get_payload, CraveCryptServer, CraveCryptError
from CraveResultsCommand import CraveResultsCommand
from CraveResultsLogType import CraveResultsLogType
from CraveResultsHyperopt import CraveResultsHyperopt
from CraveResultsSharedStatus import CraveResultsSharedStatus
from SqlDb import CraveResultsSql, command_sqldb_mapping
from GzipRotator import GZipRotator, GzipRotatingFileHandler
import struct
from collections import deque


class Aggregator:
    map = {
        's': CraveResultsSql.log_summary,
        'h': CraveResultsSql.log_history,
        'a': CraveResultsSql.log_artifact,
    }

    def __init__(self):
        self.data = {}
        self.start_time = time.time()

    def log(self, data):
        self.log_summary(copy.deepcopy(data))
        self.log_history(data)

    def log_summary(self, data):
        table_name, run_time = self._get_info(data)

        d = self.data[table_name][run_time]

        if 's' not in d:
            d['s'] = {}
        d['s'].update(data)

    def log_history(self, data):
        table_name, run_time = self._get_info(data)

        d = self.data[table_name][run_time]
        if 'h' not in d:
            d['h'] = {}
        d = d['h']

        self._update_history(data, d)

    def log_artifact(self, data):
        table_name, run_time = self._get_info(data)

        d = self.data[table_name][run_time]
        if 'a' not in d:
            d['a'] = {}
        d = d['a']
        self._update_history(data, d)

    @staticmethod
    def _update_history(data, d):
        for field in data:
            if field not in d:
                d[field] = str(data[field])
            else:
                d[field] += ", " + str(data[field])

    def _get_info(self, data):
        table_name = data['__experiment']
        run_time = data['__run_time']
        del data['__experiment']
        del data['__run_time']
        if table_name not in self.data:
            self.data[table_name] = {}

        if run_time not in self.data[table_name]:
            self.data[table_name][run_time] = {}

        return table_name, run_time

    def commit(self, log_):
        for table_name in self.data:
            for run_time in self.data[table_name]:
                sql_db = CraveResultsSql(log_)
                d = self.data[table_name][run_time]
                for t in ['s', 'h', 'a']:
                    if t in d:
                        d[t]['__experiment'] = table_name
                        d[t]['__run_time'] = run_time
                        Aggregator.map[t](sql_db, d[t])
        self.data = {}


class ThreadedServer(object):
    def __init__(self, port):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('0.0.0.0', self.port))
        self.threads = []
        self.save_dir = "/var/lib/crave_results_db/files"
        self.dataset_dir = "/var/lib/crave_results_db/datasets"
        self.all_files = {}
        self.all_files_lock = threading.Lock()
        for d in os.listdir(self.save_dir):
            d = os.path.join(self.save_dir, d)
            if os.path.isdir(d):
                for f in os.listdir(d):
                    self.all_files[os.path.basename(f)] = os.path.join(d, f)

        self.hyperopt = CraveResultsHyperopt(log)
        self.shared_status = CraveResultsSharedStatus(log)
        self.shared_status_lock = threading.Lock()
        self.crypt = CraveCryptServer(log)
        self.periodic_save_cbs = [self.join_threads, self.hyperopt.save_data, self.shared_status.save_data]
        self.periodic_save_i = 0
        self.periodic_save_cbs2 = [self.save_new_work, self.crypt.refresh_keys]
        self.interrupt_cbs = [self.hyperopt.save_data, self.shared_status.save_data, self.join_inserter]
        self.stopping = threading.Event()
        self.new_work = deque()
        self.new_work_file = "new_work_data.pickle"
        self.new_work_changed = False
        self.new_work_len = 0
        self.periodic_save_time = time.time() + 30.
        if os.path.isfile(self.new_work_file):
            with open(self.new_work_file, "rb") as fi:
                try:
                    self.new_work = pickle.loads(fi.read())
                except pickle.UnpicklingError as e:
                    log.error("Failed reading data from file: %s" % str(e))
                    self.new_work = deque()
        self.new_work_lock = threading.Lock()

        signal.signal(signal.SIGINT, self.interrupt_handler)
        signal.signal(signal.SIGTERM, self.interrupt_handler)
        log.info("Starting")
        self.threads_inserter = []
        inserter = threading.Thread(target=self.insert_thread, args=())
        inserter.start()
        self.threads_inserter.append(inserter)

    def join_inserter(self):
        log.debug("Stopping inserter threads")
        threads_joined = []
        for thread in self.threads_inserter:
            log.debug("Waiting for thread %s" % str(thread))
            thread.join(None)
            if not thread.is_alive():
                threads_joined.append(thread)
        for thread in threads_joined:
            self.threads_inserter.remove(thread)
        if len(threads_joined) > 0:
            log.info("Joined %d thread(s)" % len(threads_joined))
        if len(self.threads_inserter):
            log.info("%d threads still alive" % len(self.threads_inserter))
        self.save_new_work()

    def save_new_work(self):
        self.new_work_lock.acquire()
        if self.new_work_changed:
            if len(self.new_work):
                log.info("Saving new work")
                with open(self.new_work_file, "wb") as fi:
                    fi.write(pickle.dumps(self.new_work))
            else:
                log.info("No new work, removing save file, if exists")
                if os.path.isfile(self.new_work_file):
                    os.unlink(self.new_work_file)
            self.new_work_changed = False
        self.new_work_lock.release()

    def join_threads(self, wait=False):
        threads_joined = []
        for thread in self.threads:
            if wait:
                log.debug("Waiting for thread %s" % str(thread))
                thread.join(None)
            else:
                thread.join(0.)
            if not thread.is_alive():
                threads_joined.append(thread)
        for thread in threads_joined:
            self.threads.remove(thread)
        if len(threads_joined) > 0:
            log.info("Joined %d thread(s)" % len(threads_joined))
        if wait and len(self.threads):
            log.info("%d threads still alive" % len(self.threads))

    def periodic_save(self):
        for cb in self.periodic_save_cbs:
            cb()
        self.periodic_save_i += 1
        if self.periodic_save_i % 10 == 0:
            self.periodic_save_i = 0
            for cb in self.periodic_save_cbs2:
                cb()

    def interrupt_handler(self, sig, frame):
        log.info("Interrupted.")
        self.stopping.set()
        self.join_threads(wait=True)

        for cb in self.interrupt_cbs:
            cb()
        log.info("Exiting.")
        sys.exit(0)

    def listen(self):
        self.sock.listen(50)
        self.sock.settimeout(1.)
        while True:
            # Periodical cleanup and saving data
            if self.periodic_save_time < time.time():
                self.periodic_save_time = time.time() + 30.
                self.periodic_save()
            connection = None
            try:
                connection, client_address = self.sock.accept()
                if self.stopping.is_set():
                    log.info("Closing connection from client %s:%s as server is stopping" %
                             (client_address[0], client_address[1]))
                    connection.close()
                    continue
                connection.settimeout(60)
                log.debug('Connection from %s:%s' % (client_address[0], client_address[1]))
                thread = threading.Thread(target=self.accept, args=(connection, client_address))
                thread.start()
                self.threads.append(thread)
            except socket.timeout:
                if connection and not connection._closed:
                    connection.close()

    def insert_thread(self):
        log.info("Starting insert_thread")
        aggregating = False
        aggregator = Aggregator()
        while True:
            if self.stopping.is_set():
                if aggregating:
                    log.info("Stopping, committing aggregated data %.2f" % (time.time() - aggregator.start_time))
                    start_time = time.time()
                    aggregator.commit(log)
                    log.debug("Aggregator commit time: %.5f" % (time.time() - start_time))
                else:
                    log.info("Stopping insert_thread, not aggregating.")
                break
            start_time = time.time()
            self.new_work_lock.acquire()
            lock_acquire_time = time.time() - start_time
            if len(self.new_work):
                data = self.new_work.popleft()
                self.new_work_len = num_left = len(self.new_work)
                self.new_work_changed = True
                self.new_work_lock.release()
                if not aggregating and num_left > 100:
                    log.debug("Enabling aggregator num_left")
                    aggregating = True
                    aggregator.start_time = time.time()
            else:
                num_left = self.new_work_len
                self.new_work_lock.release()
                if aggregating and time.time() - aggregator.start_time > 60. and num_left == 0:
                    log.info("Committing aggregated data %.2f" % (time.time() - aggregator.start_time))
                    start_time = time.time()
                    aggregator.commit(log)
                    aggregating = False
                    log.debug("Aggregator commit time: %.5f" % (time.time() - start_time))
                else:
                    time.sleep(1.)
                continue
            start_time = time.time()
            sql_db = CraveResultsSql(log)
            for portion in data:
                command = struct.unpack("!b", portion[0:1])[0]
                portion_start_time = time.time()
                if command == CraveResultsLogType.INIT:
                    log.info("Processing CraveResultsLogType.INIT")
                    sql_db.init(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.HYPERPARAMS:
                    log.info("Processing CraveResultsLogType.HYPERPARAMS")
                    sql_db.config(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.LOG:
                    log.info("Processing CraveResultsLogType.LOG")
                    if aggregating:
                        aggregator.log(pickle.loads(portion[1:]))
                    else:
                        sql_db.log(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.LOG_SUMMARY:
                    log.info("Processing CraveResultsLogType.LOG_SUMMARY")
                    if aggregating:
                        aggregator.log_summary(pickle.loads(portion[1:]))
                    else:
                        sql_db.log_summary(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.LOG_HISTORY:
                    log.info("Processing CraveResultsLogType.LOG_HISTORY")
                    if aggregating:
                        aggregator.log_history(pickle.loads(portion[1:]))
                    else:
                        sql_db.log_history(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.LOG_ARTIFACT:
                    log.info("Processing CraveResultsLogType.LOG_ARTIFACT")
                    if aggregating:
                        aggregator.log_artifact(pickle.loads(portion[1:]))
                    else:
                        sql_db.log_artifact(pickle.loads(portion[1:]))
                elif command == CraveResultsLogType.BINARY:
                    log.info("Processing CraveResultsLogType.BINARY")
                    data_len = struct.unpack("!L", portion[1:5])[0]
                    pickle_len = struct.unpack("!H", portion[5:7])[0]
                    log.info("Data len: %d pickle len: %d" % (data_len, pickle_len))
                    file_contents = portion[7:7 + data_len]
                    blake = hashlib.blake2b()
                    blake.update(file_contents)
                    checksum = blake.hexdigest()
                    if not len(portion[7 + data_len:]) - 4 == pickle_len:
                        log.error("Pickle length invalid. Expected %d got %d" %
                                  (pickle_len, len(portion[7 + data_len:]) - 4))
                    data = pickle.loads(portion[7 + data_len:])
                    if checksum != data['checksum']:
                        log.error("checksum mismatch")
                    else:
                        log.debug("Checksum OK")
                    file_dir = os.path.join(self.save_dir, data['__experiment'])
                    if not os.path.isdir(file_dir):
                        os.mkdir(file_dir)
                    file_path = os.path.join(file_dir, data['checksum'])
                    with open(file_path, "wb") as f:
                        f.write(file_contents)
                    log.info("Saved data as %s" % file_path)
                    self.all_files_lock.acquire()
                    self.all_files[checksum] = file_path
                    self.all_files_lock.release()
                    sql_db.log_file(data)
                elif command == CraveResultsLogType.REMOVE_EXPERIMENT:
                    name = pickle.loads(portion[1:])
                    sql_db = CraveResultsSql(log)
                    sql_db.remove_experiment(name)
                    log.info("Removed experiment %s" % name)
                    saved_files_path = os.path.join(self.save_dir, name)
                    if os.path.isdir(saved_files_path):
                        shutil.rmtree(saved_files_path)
                        log.info("Removed directory %s" % saved_files_path)
                else:
                    log.error("Unknown CraveResultsLogType: %s" % str(command))
                log.debug("Processed portion in %.5f seconds" % (time.time() - portion_start_time))
                if not aggregating and time.time() - portion_start_time > 0.2:
                    log.debug("Enabling aggregator portion")
                    aggregating = True
                    aggregator.start_time = time.time()
            log.debug("Packet processing time: %.5f (lock: %.5f, left: %d)" %
                      ((time.time() - start_time), lock_acquire_time, num_left))
            if aggregating and time.time() - aggregator.start_time > 60. and num_left == 0:
                log.info("Committing aggregated data %.2f" % (time.time() - aggregator.start_time))
                start_time = time.time()
                aggregator.commit(log)
                aggregating = False
                log.debug("Aggregator commit time: %.5f" % (time.time() - start_time))
        log.info("Leaving insert_thread")

    def accept(self, connection, client_address):
        client_name = None
        try:
            received = connection.recv(65535)
            log.debug('Received %d bytes from %s:%s' % (len(received), client_address[0], client_address[1]))
            if len(received) == 0:
                log.info("Client closed connection. Dropping session.")
                connection.close()
                return
            request_type = struct.unpack("!b", received[0:1])[0]

            if request_type == CraveResultsCommand.UPLOAD_RESULTS:
                log.info("Processing CraveResultsCommand.UPLOAD_RESULTS")
                self.accept_results(connection, client_address, received[1:])

            elif request_type in list(command_sqldb_mapping.keys()):
                log.info("Processing %s" % str(CraveResultsCommand(request_type)))
                self._return_data(connection, client_address, request_type, received[1:],
                                  command_sqldb_mapping[request_type])

            elif request_type == CraveResultsCommand.GET_FILE:
                log.info("Processing CraveResultsCommand.GET_FILE")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for getting file")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                request = pickle.loads(payload)
                if 'checksum' not in request:
                    self._return_error(connection, client_address,
                                       "'checksum' must be in request", client_name)
                    return
                if request['checksum'] not in self.all_files:
                    self._return_error(connection, client_address,
                                       "File with requested checksum does not exist on server", client_name)
                    return
                with open(self.all_files[request['checksum']], "rb") as f:
                    file_contents_encrypted = self.crypt.encrypt(client_name, f.read())

                connection.sendall(
                    struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(file_contents_encrypted)) +
                    file_contents_encrypted)
                connection.close()

            elif request_type == CraveResultsCommand.GET_ARTIFACT:
                log.info("Processing CraveResultsCommand.GET_ARTIFACT")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for removing experiment")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                request = pickle.loads(payload)
                if 'experiment' not in request or 'field' not in request or 'run_id' not in request:
                    self._return_error(connection, client_address,
                                       "Request must contain experiment: str, field: str, run_id: float", client_name)
                    return
                sql_db = CraveResultsSql(log)
                data = sql_db.get_artifact(**request)
                if data['checksum'] not in self.all_files:
                    log.error("File not found for %s" % str(request))
                    self._return_error(connection, client_address,
                                       "Requested file has not found in storage.", client_name)
                    return
                with open(self.all_files[data['checksum']], "rb") as f:
                    file_contents_encrypted = self.crypt.encrypt(client_name, f.read())

                connection.sendall(
                    struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(file_contents_encrypted)) +
                    file_contents_encrypted)
                connection.close()

            elif request_type == CraveResultsCommand.REMOVE_EXPERIMENT:
                log.info("Processing CraveResultsCommand.REMOVE_EXPERIMENT")

                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for removing experiment")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)

                try:
                    CraveResultsSql.validate_string(name)
                except ValueError as e:
                    self._return_error(connection, client_address,
                                       "Invalid table name given: %s" % str(e), client_name)
                    return

                return_data = self.crypt.encrypt(client_name, pickle.dumps(True))
                connection.sendall(
                    struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                connection.close()

                self.new_work_lock.acquire()
                self.new_work.append([struct.pack("!b", CraveResultsLogType.REMOVE_EXPERIMENT) + payload])
                self.new_work_len = len(self.new_work)
                self.new_work_changed = True
                self.new_work_lock.release()

            elif request_type == CraveResultsCommand.LIST_HYPEROPT:
                log.info("Processing CraveResultsCommand.LIST_HYPEROPT")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for listing hyperopt")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                return_data = self.crypt.encrypt(client_name, pickle.dumps(self.hyperopt.list_hyperopt()))
                connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                connection.close()

            elif request_type == CraveResultsCommand.REMOVE_HYPEROPT:
                log.info("Processing CraveResultsCommand.REMOVE_HYPEROPT")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address, "Incomplete message received for removing hyperopt")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)
                return_data = self.hyperopt.remove_hyperopt(name)
                if not return_data:
                    self._return_error(connection, client_address, "Failed to remove hyperopt. Does it exist?",
                                       client_name)
                    return
                else:
                    return_data = self.crypt.encrypt(client_name, pickle.dumps(return_data))
                    connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK,
                                                   len(return_data)) + return_data)
                    connection.close()

            elif request_type == CraveResultsCommand.GET_HYPEROPT:
                log.info("Processing CraveResultsCommand.GET_HYPEROPT")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address, "Incomplete message received for getting hyperopt")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)
                return_data = self.crypt.encrypt(client_name, pickle.dumps(self.hyperopt.get(name)))
                connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                connection.close()

            elif request_type == CraveResultsCommand.PUT_HYPEROPT:
                log.info("Processing CraveResultsCommand.PUT_HYPEROPT")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address, "Incomplete message received for updating hyperopt")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name, data = pickle.loads(payload)
                self.hyperopt.lock.acquire()
                try:
                    result = self.hyperopt.update(name, data)
                    if not result:
                        self._return_error(connection, client_address,
                                           "Name does not exist. do get_hyperopt() first", client_name)
                        return
                    else:
                        return_data = self.crypt.encrypt(client_name, pickle.dumps(result))
                        connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) +
                                           return_data)
                        connection.close()
                except Exception as e:
                    log.error('Error updating hyperopt for %s' % name)
                    log.exception(e)
                finally:
                    self.hyperopt.lock.release()
            elif request_type == CraveResultsCommand.LIST_SHARED_STATUS:
                log.info("Processing CraveResultsCommand.LIST_SHARED_STATUS")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for listing shared status")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                return_data = self.crypt.encrypt(client_name, pickle.dumps(self.shared_status.list()))
                connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                connection.close()

            elif request_type == CraveResultsCommand.UPDATE_SHARED_STATUS:
                log.info("Processing CraveResultsCommand.UPDATE_SHARED_STATUS")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for getting shared status")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)
                self.shared_status_lock.acquire()
                shared_status = self.shared_status.get(name)
                if shared_status is None:
                    self._return_error(connection, client_address,
                                       "Shared status with this name is not initialized", client_name)
                    self.shared_status_lock.release()
                    return
                return_data = self.crypt.encrypt(client_name, pickle.dumps(shared_status))
                try:
                    connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                    log.debug("Sent %d bytes (w/o header), waiting for answer" % len(return_data))
                    connection.settimeout(20.)
                    received = connection.recv(65535)
                except socket.timeout:
                    log.warning("Timeout waiting for status update.")
                    self.shared_status_lock.release()
                    connection.close()
                    return
                except socket.error as e:
                    log.warning("Socket error: %s" % str(e))
                    self.shared_status_lock.release()
                    connection.close()
                    return
                log.debug('Received %d bytes from %s:%s' % (len(received), client_address[0], client_address[1]))
                if len(received) == 0:
                    log.info("Client closed connection. Dropping session.")
                    connection.close()
                    self.shared_status_lock.release()
                    return
                payload = _get_payload(log, connection, client_address, received)
                client_name, payload = self.crypt.decrypt(payload)
                name, new_data = pickle.loads(payload)
                if not self.shared_status.update(name, new_data):
                    self.shared_status_lock.release()
                    self._return_error(connection, client_address,
                                       "Incomplete message received for updating shared status", client_name)
                    return
                self.shared_status_lock.release()
                return_data = self.crypt.encrypt(client_name, pickle.dumps(True))
                connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK,
                                               len(return_data)) + return_data)
                connection.close()

            elif request_type == CraveResultsCommand.CREATE_SHARED_STATUS:
                log.info("Processing CraveResultsCommand.CREATE_SHARED_STATUS")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for updating shared status")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name, value = pickle.loads(payload)
                self.shared_status.lock.acquire()
                self.shared_status.create(name, value)
                self.shared_status.lock.release()
                return_data = self.crypt.encrypt(client_name, pickle.dumps(True))
                connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK,
                                               len(return_data)) + return_data)
                connection.close()

            elif request_type == CraveResultsCommand.REMOVE_SHARED_STATUS:
                log.info("Processing CraveResultsCommand.REMOVE_SHARED_STATUS")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for removing shared status")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)
                return_data = self.shared_status.remove(name)
                if not return_data:
                    self._return_error(connection, client_address, "Failed to remove shared status. Does it exist?",
                                       client_name)
                    return
                else:
                    return_data = self.crypt.encrypt(client_name, pickle.dumps(return_data))
                    connection.sendall(struct.pack("!bL", CraveResultsCommand.COMMAND_OK,
                                                   len(return_data)) + return_data)
                    connection.close()

            elif request_type == CraveResultsCommand.GET_DATASET:
                # TODO Send multiple files in one request
                log.info("Processing CraveResultsCommand.GET_DATASET")
                payload = _get_payload(log, connection, client_address, received[1:])
                if payload is None:
                    self._return_error(connection, client_address,
                                       "Incomplete message received for downloading dataset")
                    return
                client_name, payload = self.crypt.decrypt(payload)
                name = pickle.loads(payload)
                if not len(name):
                    self._return_error(connection, client_address, "File name not given",
                                       client_name)
                    return
                if name[0] == '/':
                    self._return_error(connection, client_address, "Only relative path is accepted.",
                                       client_name)
                    return
                filepath = os.path.join(self.dataset_dir, name)
                if not os.path.isfile(filepath):
                    self._return_error(connection, client_address, "File does not exist.",
                                       client_name)
                    return
                download_start_time = time.time()
                with open(filepath, "rb") as f:
                    file_contents_encrypted = self.crypt.encrypt(client_name, f.read())

                length = len(file_contents_encrypted)
                connection.settimeout(120)
                connection.sendall(
                    struct.pack("!bL", CraveResultsCommand.COMMAND_OK, length) +
                    file_contents_encrypted)
                total_time = time.time() - download_start_time
                if total_time > 0.1:
                    log.debug("Uploaded in %.4f seconds, %.1f Mbps" % (total_time, (length/1e6)/total_time))
                connection.close()
            else:
                self._return_error(connection, client_address, "Message type %d not implemented" % request_type)
        except Exception as e:
            log.exception(e)
            if connection and not connection._closed:
                self._return_error(connection, client_address, "Failed processing request", client_name)

    def _return_data(self, connection, client_address, request_type, request_data, request_callback):
        sql_db = CraveResultsSql(log)
        if request_data:
            client_name, request_params = self.crypt.decrypt(request_data)
            request_params = pickle.loads(request_params)
            if 'dummy' in request_params:
                return_data = pickle.dumps(request_callback(sql_db))
            else:
                return_data = pickle.dumps(request_callback(sql_db, **request_params))
            return_data = self.crypt.encrypt(client_name, return_data)
            connection.sendall(struct.pack("!bL", request_type, len(return_data)) + return_data)
            connection.close()
        else:
            self._return_error(connection, client_address, "Empty message received")

    def _return_error(self, connection, client_address, message, client_name=None):
        try:
            if client_name:
                return_type = CraveResultsCommand.COMMAND_FAILED_ENCRYPTED
                return_message = self.crypt.encrypt(client_name, message.encode())
            else:
                return_type = CraveResultsCommand.COMMAND_FAILED
                return_message = message.encode()
            log.error("Returning error: %s to %s:%s" % (message, client_address[0], client_address[1]))
            return_data = struct.pack(f"!bL", return_type,
                                      len(return_message)) + return_message
            connection.sendall(return_data)
        except Exception as e:
            log.error("%d Failed sending error: %s" % (threading.get_ident(), str(e)))
            log.exception(e)
        connection.close()

    def accept_results(self, connection, client_address, new_data):
        start_time = time.time()
        payload = _get_payload(log, connection, client_address, new_data)
        if payload is None:
            self._return_error(connection, client_address, "Incomplete message received")
            return

        try:
            client, payload = self.crypt.decrypt(payload)
        except CraveCryptError as e:
            log.error("accept_results() unable to decrypt data")
            self._return_error(connection, client_address, "Unable to decrypt")
            return

        if self.new_work_len >= 10000:
            log.error("accept_results() New work queue full")
            self._return_error(connection, client_address, "New work queue full", client)
            return

        # Insert payload data to database
        data = []
        cursor = 0
        portion_num = 0
        while cursor < len(payload):
            portion_len = struct.unpack("!L", payload[cursor:cursor+4])[0]
            if len(payload[cursor+4:]) >= portion_len:
                data.append(payload[cursor+4:cursor+4+portion_len+4])
                cursor += portion_len + 4
                portion_num += 1
            else:
                log.error("%d accept_results() requested to read more than available (num: %d, %d/%d)" %
                          (threading.get_ident(), portion_num, portion_len, len(payload[cursor+4:])))
                self._return_error(connection, client_address, "Incomplete portion (num: %d, %d/%d)" %
                                   (portion_num, portion_len, len(payload[cursor+4:])), client)
                return

        return_data = struct.pack("!b", CraveResultsCommand.COMMAND_OK)
        connection.sendall(return_data)
        connection.close()

        start_time_lock = time.time()
        self.new_work_lock.acquire()
        lock_acquire_time = time.time() - start_time_lock
        self.new_work.append(data)
        self.new_work_len = len(self.new_work)
        self.new_work_changed = True
        self.new_work_lock.release()

        log.debug("Success in %.4f seconds (lock: %.4f, len: %d)." %
                  ((time.time() - start_time), lock_acquire_time, self.new_work_len))


def run_server():
    load_dotenv()
    ThreadedServer(65099).listen()


if __name__ == '__main__':
    log_file_name = "crave_results_server.log"
    loggingLevel = logging.DEBUG
    log = logging.getLogger("crave_results_server")
    log.setLevel(loggingLevel)
    logger = GzipRotatingFileHandler(filename=log_file_name, maxBytes=1024 * 1024 * 5, backupCount=10)
    logger.rotator = GZipRotator()
    logFormatter = logging.Formatter("%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    logger.setFormatter(logFormatter)
    log.addHandler(logger)
    try:
        run_server()
    except Exception as e:
        log.exception(e)
