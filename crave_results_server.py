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
from SqlDb import CraveResultsSql, command_sqldb_mapping
from GzipRotator import GZipRotator
import struct


class ThreadedServer(object):
    def __init__(self, port):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('0.0.0.0', self.port))
        self.threads = []
        self.save_dir = "/var/lib/crave_results_db/files"
        self.all_files = {}
        self.all_files_lock = threading.Lock()
        for d in os.listdir(self.save_dir):
            d = os.path.join(self.save_dir, d)
            if os.path.isdir(d):
                for f in os.listdir(d):
                    self.all_files[os.path.basename(f)] = os.path.join(d, f)

        self.hyperopt = CraveResultsHyperopt(log)
        self.periodic_save_cbs = [self.join_threads, self.hyperopt.save_data]
        self.interrupt_cbs = [self.hyperopt.save_data]

        self.crypt = CraveCryptServer(log)

        signal.signal(signal.SIGINT, self.interrupt_handler)
        signal.signal(signal.SIGTERM, self.interrupt_handler)
        signal.signal(signal.SIGALRM, self.periodic_save)
        signal.alarm(30)
        log.info("Starting")

    def join_threads(self, wait=False):
        threads_joined = []
        for thread in self.threads:
            if wait:
                thread.join(1.)
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

    def periodic_save(self, signum, stack):
        signal.alarm(30)
        for cb in self.periodic_save_cbs:
            cb()

    def interrupt_handler(self, sig, frame):
        log.info("Interrupted.")
        self.join_threads(wait=True)
        for cb in self.interrupt_cbs:
            cb()
        log.info("Exiting.")
        sys.exit(0)

    def listen(self):
        self.sock.listen(50)
        while True:
            connection, client_address = self.sock.accept()
            connection.settimeout(60)
            log.debug('Connection from %s:%s' % (client_address[0], client_address[1]))
            thread = threading.Thread(target=self.accept, args=(connection, client_address))
            thread.start()
            self.threads.append(thread)

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
                                       "'checksum' must be in request")
                    return
                if request['checksum'] not in self.all_files:
                    self._return_error(connection, client_address,
                                       "File with requested checksum does not exist on server")
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
                                       "Request must contain experiment: str, field: str, run_id: float")
                    return
                sql_db = CraveResultsSql(log)
                data = sql_db.get_artifact(**request)
                if data['checksum'] not in self.all_files:
                    log.error("File not found for %s" % str(request))
                    self._return_error(connection, client_address,
                                       "Requested file has not found in storage.")
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
                sql_db = CraveResultsSql(log)
                try:
                    CraveResultsSql.validate_string(name)
                except ValueError as e:
                    self._return_error(connection, client_address,
                                       "Invalid table name given: %s" % str(e))
                    return
                sql_db.remove_experiment(name)
                log.info("Removed experiment %s" % name)
                saved_files_path = os.path.join(self.save_dir, name)
                if os.path.isdir(saved_files_path):
                    shutil.rmtree(saved_files_path)
                    log.info("Removed directory %s" % saved_files_path)

                return_data = self.crypt.encrypt(client_name, pickle.dumps(True))
                connection.sendall(
                    struct.pack("!bL", CraveResultsCommand.COMMAND_OK, len(return_data)) + return_data)
                connection.close()

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
                    self._return_error(connection, client_address, "Incomplete message received for removing hyperopt")
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
                    self._return_error(connection, client_address, "Incomplete message received for removing hyperopt")
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
            raise ValueError("_return_data() called without request_data")

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
                                   (portion_num, portion_len, len(payload[cursor+4:])))
                return
        sql_db = CraveResultsSql(log)
        for portion in data:
            command = struct.unpack("!b", portion[0:1])[0]
            if command == CraveResultsLogType.INIT:
                log.info("Processing CraveResultsLogType.INIT")
                sql_db.init(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.HYPERPARAMS:
                log.info("Processing CraveResultsLogType.HYPERPARAMS")
                sql_db.config(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.LOG:
                log.info("Processing CraveResultsLogType.LOG")
                sql_db.log(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.LOG_SUMMARY:
                log.info("Processing CraveResultsLogType.LOG_SUMMARY")
                sql_db.log_summary(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.LOG_HISTORY:
                log.info("Processing CraveResultsLogType.LOG_HISTORY")
                sql_db.log_history(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.LOG_ARTIFACT:
                log.info("Processing CraveResultsLogType.LOG_ARTIFACT")
                sql_db.log_artifact(pickle.loads(portion[1:]))
            elif command == CraveResultsLogType.BINARY:
                log.info("Processing CraveResultsLogType.BINARY")
                data_len = struct.unpack("!L", portion[1:5])[0]
                pickle_len = struct.unpack("!H", portion[5:7])[0]
                log.info("Data len: %d pickle len: %d" % (data_len, pickle_len))
                file_contents = portion[7:7+data_len]
                blake = hashlib.blake2b()
                blake.update(file_contents)
                checksum = blake.hexdigest()
                if not len(portion[7 + data_len:]) == pickle_len:
                    log.error("Pickle length invalid. Expected %d got %d" %
                              (pickle_len, len(portion[7 + data_len:])))
                data = pickle.loads(portion[7+data_len:])
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
            else:
                message = "Unknown CraveResultsLogType: %s" % str(command)
                self._return_error(connection, client_address, message, client)
                return

        return_data = struct.pack("!b", CraveResultsCommand.COMMAND_OK)
        connection.sendall(return_data)
        connection.close()
        log.debug("Success.")


def run_server():
    load_dotenv()
    ThreadedServer(65099).listen()


if __name__ == '__main__':
    log_file_name = "crave_results_server.log"
    loggingLevel = logging.DEBUG
    log = logging.getLogger("crave_results_server")
    log.setLevel(loggingLevel)
    logger = logging.handlers.RotatingFileHandler(filename=log_file_name, maxBytes=1024 * 1024 * 5, backupCount=10)
    logger.rotator = GZipRotator()
    logFormatter = logging.Formatter("%(asctime)s %(levelname)s %(threadName)s: %(message)s")
    logger.setFormatter(logFormatter)
    log.addHandler(logger)
    run_server()
