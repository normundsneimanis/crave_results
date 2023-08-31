import time
import mariadb
import os
import copy
import json
from CraveResultsCommand import CraveResultsCommand
from typing import Dict, Union, List, Any
import posix_ipc
import re
from SharedStatus import SharedStatus


class CraveResultsException(Exception):
    pass


mariadb_variables_sizes = {
    int: {
        # Size of INT: 2**32/2-1: -2147483648 to 2147483647
        "INT": lambda a: -1 * 2**32/2-1 - 1 <= a <= 2**32/2-1,
        # Size of BIGINT: 2**64/2-1 -9223372036854775808 to 9223372036854775807
        "BIGINT": lambda a: -1 * 2**64/2-1 - 1 <= a <= 2**64/2-1,
    },
    str: {
        # Size of CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL
        "TINYTEXT":
            lambda a: a <= 2**8-1,  # <= 2^8-1, 255 chars
        "TEXT":
            lambda a: a <= 2**16-1,  # <= 2^16-1, 65535 chars
        "MEDIUMTEXT":
            lambda a: a <= 2**24-1,  # <= 2^24-1, 16'777'215 chars
        "LONGTEXT":
            lambda a: a <= 2**32-1,  # <= 2^32-1, 4'294'967'295 chars (4GB)
    },
    float: "DOUBLE",
    bool: "TINYINT"
}


type_map = {
    int: lambda a: a,
    str: lambda a: a,
    float: lambda a: a,
    bool: lambda a: int(a),
    list: lambda a: json.dumps(a),
    dict: lambda a: json.dumps(a),
    type(None): lambda a: str(a)
}


class SqlDb:
    def __init__(self, user, password, database):
        self.user = user
        self.password = password
        self.database = database
        self.open = 0
        self.cursor = None
        self.db = None

    def __enter__(self):
        if not self.open:
            self.db = mariadb.connect(
                unix_socket='/run/mysqld/mysqld.sock',
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.db.cursor()
        self.open += 1
        return [self.cursor, self.db]

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.open -= 1
        if not self.open:
            self.cursor.close()
            self.db.close()
            self.cursor = None
            self.db = None


class CraveResultsBase:
    def __init__(self, logger):
        self.logger = logger
        self.sql = SqlDb(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASS'],
            database=os.environ['DB_NAME']
        )

    def _select_db(self, query: str, lines=False) -> list:
        try:
            self.sql.cursor.execute(query)
        except mariadb.ProgrammingError as e:
            self.logger.error("Failed executing query: [%s]: %s" % (query, str(e)))
            raise CraveResultsException from e
        result = []
        for a in self.sql.cursor.fetchall():
            if lines:
                result.append(list(a))
            else:
                for b in a:
                    result.append(b)
        return result


class CraveResultsSql(CraveResultsBase):
    allowed_chars = re.compile(r"[\w_\- /]")
    # db_status = {}

    def __init__(self, log):
        super().__init__(log)
        self.table_name = None
        self.run_time = 0.0
        self.initialized = False
        self.dbname = os.environ['DB_NAME']
        self.logger = log

        self.semaphore_create_table = "crave_results_create"
        try:
            with posix_ipc.Semaphore(self.semaphore_create_table) as s:
                pass
        except posix_ipc.ExistentialError:
            self.logger.debug("Initializing create table semaphore")
            posix_ipc.Semaphore(self.semaphore_create_table, posix_ipc.O_CREAT, initial_value=1)

        self.semaphore_db = "crave_results_db"
        shmem = SharedStatus(self.semaphore_db)
        if not shmem.is_initialized():
            self.logger.debug("Initializing shared memory for database status")
            shmem.initialize({})

    def _set_metadata(self, data):
        if not self.table_name:
            self.table_name = self._rep(data['__experiment'])
        if self.run_time == 0.0:
            self.run_time = data['__run_time']
        del data['__experiment']
        del data['__run_time']

    @staticmethod
    def _validate_keys(data):
        for k in data.keys():
            CraveResultsSql.validate_string(k)

    @staticmethod
    def validate_string(s):
        invalid_chars = re.sub(CraveResultsSql.allowed_chars, "", s)
        if len(invalid_chars):
            raise ValueError("Invalid characters found in %s: %s" % (s, invalid_chars))

    def init(self, data: dict) -> None:
        self._set_metadata(data)
        with self.sql:
            # Create experiment table if it doesn't exist and insert run
            self.logger.info("Attempting to check table %s" % self.table_name)
            with posix_ipc.Semaphore(self.semaphore_create_table) as s:
                if not len(self._select_db(f"SHOW TABLES LIKE '{self.table_name}'")):
                    self.sql.cursor.execute(f"CREATE TABLE {self.table_name} "
                                            f"(id int NOT NULL AUTO_INCREMENT PRIMARY KEY)"
                                            f" ROW_FORMAT=COMPRESSED")
                    self.logger.info("Created table %s" % self.table_name)
                else:
                    self.logger.info("Table already exists: %s" % self.table_name)
            self._insert({"run_time": self.run_time})

            if 'config' in data.keys():
                self.config(data['config'])
                del(data['config'])

            data = self._prepend_key(data, "r")
            self._insert(data)
        self.initialized = True

    def config(self, data: dict) -> None:
        self._set_metadata(data)
        data = self._prepend_key(data, "c")
        # TODO bring this to _prepend_key and validate
        for k in list(data.keys()):
            if isinstance(data[k], str):
                data[k] = data[k].replace("'", '"')
        with self.sql:
            self._insert(data)

    def log(self, data: dict) -> None:
        self._set_metadata(data)
        data = copy.deepcopy(data)
        summary = self._prepend_key(data, "s")
        history = self._prepend_key(data, "h")
        self._dict_val_to_str(history)
        with self.sql:
            self._insert(summary)
            self._update(history)

    def log_history(self, data: dict) -> None:
        self._set_metadata(data)
        data = copy.deepcopy(data)
        history = self._prepend_key(data, "h")
        self._dict_val_to_str(history)
        with self.sql:
            self._update(history)

    def log_summary(self, data: dict):
        self._set_metadata(data)
        summary = self._prepend_key(data, "s")
        with self.sql:
            self._insert(summary)

    def log_file(self, data: dict):
        self._set_metadata(data)
        data = self._prepend_key(data, "z")
        with self.sql:
            self._update(data)

    def log_artifact(self, data: dict) -> None:
        self._set_metadata(data)
        # Log run artifacts, such as log files, images or video
        data = self._prepend_key(data, "f")
        with self.sql:
            self._update(data)

    def remove_experiment(self, name):
        self.validate_string(self._rep(name))

        shmem = None
        try:
            shmem = SharedStatus(self.semaphore_db)
            shmem.lock()
            db_status = shmem.get_locked()
            if name in db_status.keys():
                self.logger.info("remove_experiment(): Removing %s from db_status" % name)
                del db_status[name]
            else:
                self.logger.warning("remove_experiment(): %s does not exist in db_status" % name)
            shmem.put_locked(db_status)
            with self.sql:
                self.sql.cursor.execute("drop table if exists %s" % name)
        finally:
            if shmem:
                shmem.unlock()

    # Replace spaces and hyphens with SQL compatible characters.
    @staticmethod
    def _rep(field):
        return field.replace(" ", "_").replace("-", "_").replace('/', '__')

    @staticmethod
    def _prepend_key(data: dict, key) -> dict:
        data = copy.deepcopy(data)
        for k in list(data.keys()):
            CraveResultsSql.validate_string(k)
            data[key + CraveResultsSql._rep(k)] = data[k]
            del data[k]

        return data

    @staticmethod
    def _dict_val_to_str(data: dict) -> None:
        for k in list(data.keys()):
            data[k] = str(data[k]).replace("'", '"')

    @staticmethod
    def find_record_type(field: str, t: type, length: int) -> str:
        for typ in mariadb_variables_sizes.keys():
            if t == typ:
                if type(mariadb_variables_sizes[typ]) == dict:
                    for field_type in mariadb_variables_sizes[typ]:
                        if mariadb_variables_sizes[typ][field_type](length):
                            return field_type
                else:
                    return mariadb_variables_sizes[typ]
        raise ValueError("Failed to determine field type for %s: %s" % (field, t))

    @staticmethod
    def record_type_idx(field: str, t: type) -> int:
        # return record type index (ordered from smallest to largest)
        for typ in mariadb_variables_sizes.keys():
            if t == typ:
                if type(mariadb_variables_sizes[typ]) == dict:
                    return list(mariadb_variables_sizes[typ].keys()).index(field.upper())
                else:
                    return 0
        raise ValueError("Failed to determine field order for %s: %s" % (field, t))

    def _update(self, data: dict) -> None:
        with self.sql:
            self.__insert(data, update=True)

    def _insert(self, data: dict) -> None:
        with self.sql:
            self.__insert(data)

    def __insert(self, data: dict, update=False) -> None:
        if not len(data.keys()):
            return
        shmem = None
        stats = {}
        try:
            shmem = SharedStatus(self.semaphore_db)
            # lock_wait = time.time()
            shmem.lock()
            # self.logger.info("Lock wait time: %.4f" % (time.time()-lock_wait))

            start_time = time.time()
            db_status = shmem.get_locked()
            stats['db_status_time'] = "%.4f" % (time.time() - start_time)
            # db_status = CraveResultsSql.db_status
            start_time = time.time()
            # self.logger.debug("table_name: %s, db_status: %s" % (str(self.table_name), str(db_status)))
            if self.table_name not in db_status or 'existing_columns' not in db_status[self.table_name]:
                q = f'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ' \
                    f'\'{self.dbname}\' AND TABLE_NAME = \'{self.table_name}\';'
                columns = self._select_db(q, lines=True)
                existing_columns = {i[0]: i[1] for i in columns}
                db_status[self.table_name] = {}
                db_status[self.table_name]['existing_columns'] = existing_columns
                db_status[self.table_name]['lengths'] = {}
                db_status[self.table_name]['run_times_ids'] = {}

            else:
                existing_columns = db_status[self.table_name]['existing_columns']
            stats['existing_columns_time'] = "%.4f" % (time.time() - start_time)

            if self.run_time not in db_status[self.table_name]['lengths']:
                db_status[self.table_name]['lengths'][self.run_time] = {}

            added_columns = []
            added_columns_types = []
            added_columns_lengths = []
            new_columns_length = {}
            modify_columns = {}
            queries = []

            start_time = time.time()
            for column in data.keys():
                data[column] = type_map[type(data[column])](data[column])
                if type(data[column]) == str:
                    data_length = len(data[column])
                    if update:
                        data_length += 2
                elif type(data[column]) == float or type(data[column]) == int:
                    data_length = data[column]
                else:
                    raise CraveResultsException("Incorrect data type")
                if update and column in list(existing_columns.keys()):
                    if column not in db_status[self.table_name]['lengths'][self.run_time]:
                        current_maxlen = 0
                    else:
                        current_maxlen = db_status[self.table_name]['lengths'][self.run_time][column]
                    data_length += current_maxlen
                    new_columns_length[column] = data_length
                field_type = self.find_record_type(column, type(data[column]), data_length)
                # self.logger.debug("column %s field_type %s data len: %d" % (column, field_type, data_length))
                if column not in list(existing_columns.keys()) and column not in added_columns:
                    queries.append(f"ALTER TABLE {self.table_name} ADD COLUMN {column} {field_type}")
                    added_columns.append(column)
                    added_columns_types.append(field_type)
                    added_columns_lengths.append(data_length)
                elif column not in modify_columns.keys():
                    # Determine column type and create/alter database table if needed
                    if field_type.lower() != existing_columns[column].lower():
                        try:
                            if self.record_type_idx(field_type, type(data[column])) > \
                                    self.record_type_idx(existing_columns[column], type(data[column])):
                                queries.append(f"ALTER TABLE {self.table_name} MODIFY {column} {field_type}")
                                modify_columns[column] = field_type
                        except ValueError as e:
                            self.logger.error("Field type changed for column %s" % column)
            stats['modify_prepare_times'] = "%.4f" % (time.time() - start_time)

            start_time = time.time()
            for q in queries:
                try:
                    self.sql.cursor.execute(q)
                except mariadb.DataError as e:
                    self.logger.error("SQL Error in data for alterdb query [%s]" % q)
                    self.logger.exception(e)
                    raise Exception("Exception [%s] in alterdb query %s: " % (str(e), "")) from e
                except mariadb.ProgrammingError as e:
                    raise Exception("Exception [%s] in query %s: " % (str(e), q)) from e
            self.sql.db.commit()
            stats['modify_times'] = "%.4f" % (time.time() - start_time)

            if 'error' in data.keys():
                data['error'] = data['error'].replace("\'", "\\'")

            queries = []
            row_count = 0
            start_time = time.time()
            for column in data.keys():
                if column == 'run_time':
                    self.sql.cursor.execute(f"INSERT INTO {self.table_name} ({column}) VALUES ({data[column]})")
                    db_status[self.table_name]['run_times_ids'][self.run_time] = self.sql.cursor.lastrowid
                    continue
                if self.run_time not in db_status[self.table_name]['run_times_ids']:
                    ids = self._select_db(f"select id from {self.table_name} where run_time = '{self.run_time}'")
                    if not len(ids):
                        # Run with run_time does not exist in database because table was removed.
                        # Silently ignore old records.
                        continue
                    db_status[self.table_name]['run_times_ids'][self.run_time] = ids[0]
                if update:
                    if column not in list(db_status[self.table_name]['lengths'][self.run_time].keys()):
                        # self.logger.debug("Attempting to insert data for %s %s" % (column, str(self.run_time)))
                        queries.append(f"UPDATE {self.table_name} SET {column}='{data[column]}' "
                                       f"WHERE id={db_status[self.table_name]['run_times_ids'][self.run_time]}")
                    else:
                        # self.logger.debug("Attempting to concat data for %s %s" % (column, str(self.run_time)))
                        queries.append(f"UPDATE {self.table_name} SET {column}=CONCAT({column}, ', {data[column]}') "
                                       f"WHERE id={db_status[self.table_name]['run_times_ids'][self.run_time]}")
                else:
                    queries.append(f"UPDATE {self.table_name} SET {column}='{data[column]}' "
                                   f"WHERE id={db_status[self.table_name]['run_times_ids'][self.run_time]}")
            stats['insert_prepare_queries_time'] = "%.4f" % (time.time() - start_time)

            start_time = time.time()
            for i, column in enumerate(added_columns):
                self.logger.debug("Adding %s %s column %s type %s with length %s" %
                                  (self.table_name, str(self.run_time), column, added_columns_types[i],
                                   added_columns_lengths[i]))
                db_status[self.table_name]['existing_columns'][column] = added_columns_types[i]
                db_status[self.table_name]['lengths'][self.run_time][column] = added_columns_lengths[i]

            for column in list(new_columns_length.keys()):
                db_status[self.table_name]['lengths'][self.run_time][column] = new_columns_length[column]

            for column in list(modify_columns.keys()):
                self.logger.debug("Modifying %s column from %s %s to %s" %
                                  (self.table_name, column, db_status[self.table_name]['existing_columns'][column],
                                   modify_columns[column]))
                db_status[self.table_name]['existing_columns'][column] = modify_columns[column]
            stats['modified_put_to_dbstatus_time'] = "%.4f" % (time.time() - start_time)
            start_time = time.time()
            shmem.put_locked(db_status)
            stats['shmem_put_time'] = "%.4f" % (time.time() - start_time)

            start_time = time.time()
            for q in queries:
                try:
                    self.sql.cursor.execute(q)
                except mariadb.DataError as e:
                    self.logger.error("SQL Error in data for query [%s]" % q)
                    self.logger.exception(e)
                    raise Exception("Exception [%s] in query %s: " % (str(e), q)) from e
                except mariadb.ProgrammingError as e:
                    self.logger.error("SQL Error in query [%s]" % q)
                    self.logger.exception(e)
                    raise Exception("Exception [%s] in query %s: " % (str(e), "")) from e
                except mariadb.OperationalError as e:
                    self.logger.error("SQL Error in query [%s]" % q)
                    self.logger.exception(e)
                    raise Exception("Exception [%s] in query %s: " % (str(e), "")) from e
                row_count += 1
            self.sql.db.commit()
            stats['insert_commit_time'] = "%.4f" % (time.time() - start_time)
            # self.logger.debug("Timing: " + str(stats))
            return
        except Exception as e:
            self.logger.error("Exception while inserting in database: %s" % str(e))
            self.logger.exception(e)
            raise CraveResultsException("Exception while inserting in database: %s" % str(e)) from e
        finally:
            if shmem:
                shmem.unlock()

    def get_experiments(self) -> list:
        with self.sql:
            return self._select_db("show tables")

    def get_runs(self, experiment: str, run_identified_by: str = "") -> list:
        with self.sql:
            if run_identified_by:
                return self._select_db(f'select id, run_time, {"r" + run_identified_by} from `{experiment}`', True)
            else:
                return self._select_db(f'select id, run_time from `{experiment}`', True)

    # Get run fields
    def get_fields(self, experiment: str) -> dict:
        #   [results db|config|summary|history|file|zfile|gpu|vmstat]_<field_name>
        #   [r|c|s|h|f|z|g|v]<field_name>
        columns_mapping = ["run", "config", "summary", "history", "file", "zfile", "gpu", "vmstat"]
        special_fields = ['id', 'run_time']
        with self.sql:
            columns = self._select_db(f'SHOW columns FROM `{experiment}`', True)
            fields = [i[0] for i in columns]
        result = {}
        for m in columns_mapping:
            result[m] = []
        for field in fields:
            if field in special_fields:
                continue
            for m in columns_mapping:
                if field.startswith(m[0]):
                    result[m].append(field[1:])

        return result

    def get_rows(self, experiment: str, rows: dict, special=False) -> Dict[str, Union[List[str], List[Any]]]:
        fields = []
        fields_ret = []
        for key in rows.keys():
            for field in rows[key]:
                fields.append(key[0] + field)
                fields_ret.append(field)
        if special:
            fields += ['id', 'run_time']
            fields_ret += ['id', 'run_time']
        fields = ", ".join(fields)
        with self.sql:
            result = self._select_db(f'select {fields} from `{experiment}`', lines=True)

        to_remove_indexes = []
        for row in result:
            for idx, field in enumerate(row):
                if type(field) == str and (field[0] == "{" or field[0] == "["):
                    if idx not in to_remove_indexes:
                        to_remove_indexes.append(idx)

        for idx in reversed(to_remove_indexes):
            for i, row in enumerate(result):
                del row[idx]
            del fields_ret[idx]

        res_ = []
        for row in result:
            r = {}
            for i in range(len(row)):
                r[fields_ret[i]] = row[i]
            res_.append(r)

        return res_

    def get_field(self, experiment: str, field: str, run_id: int) -> list:
        with self.sql:
            return self._select_db(f'select {field} from `{experiment}` where id={run_id}')

    def get_artifact(self, experiment: str, field: str, run_id: int) -> dict:
        with self.sql:
            return self._process_data(self._select_db(
                f'select f{field} from `{experiment}` where id={run_id}'), arr=False)

    def get_history(self, experiment: str, field: str, run_id: int) -> list:
        with self.sql:
            return self._process_data(self._select_db(
                f'select {"h"+field} from `{experiment}` where id={run_id}'))

    def get_history_(self, experiment: str, field: str, run_time: float) -> list:
        with self.sql:
            return self._process_data(self._select_db(
                f'select {"h"+field} from `{experiment}` where run_time={run_time}'))

    def get_summary(self, experiment: str, field: str, run_id: int) -> list:
        with self.sql:
            return self._select_db(f'select {"s"+field} from `{experiment}` where id={run_id}')

    @staticmethod
    def _process_data(data, arr=True):
        if data[0] is None:
            return data
        if not data:
            if arr:
                return []
            return ""
        if data[0][0] == "{":
            try:
                if arr:
                    return json.loads('['+data[0]+']', strict=False)
                else:
                    return json.loads(data[0], strict=False)
            except json.JSONDecodeError:
                pass
        elif data[0][0] == "[":
            try:
                if arr:
                    return json.loads('['+data[0]+']', strict=False)
                else:
                    return json.loads(data[0], strict=False)
            except json.JSONDecodeError:
                pass
        try:
            float(data[0][0])
            return [float(i) for i in data[0].split(", ")]
        except ValueError:
            pass
        if arr:
            return data[0].split(", ")
        else:
            return data


command_sqldb_mapping = {
    CraveResultsCommand.LIST_EXPERIMENTS: CraveResultsSql.get_experiments,
    CraveResultsCommand.GET_RUNS: CraveResultsSql.get_runs,
    CraveResultsCommand.GET_FIELDS: CraveResultsSql.get_fields,
    CraveResultsCommand.GET_FIELD: CraveResultsSql.get_field,
    CraveResultsCommand.GET_ROW: CraveResultsSql.get_rows,
    CraveResultsCommand.GET_HISTORY_BY_ID: CraveResultsSql.get_history,
    CraveResultsCommand.GET_HISTORY_BY_TIME: CraveResultsSql.get_history_,
    CraveResultsCommand.GET_SUMMARY: CraveResultsSql.get_summary,
}
