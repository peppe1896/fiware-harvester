import sqlite3
import ast
import shutil
import datetime
import os
import mysql.connector
import json


class Db_schema_helper:
    def __init__(self, db_folder, backup_folder, count_limit=10):
        self.base_folder = db_folder
        self.db_uri = self.base_folder + "db_schema.db"
        self.connector_sqlite = sqlite3.connect(self.db_uri)
        self.cursor_sqlite = self.connector_sqlite.cursor()
        self.connector_mysql = mysql.connector.connect(
            host="localhost",
            user="peppe1896",
            password="Password1!",
            database="smartdatamodels"
        )
        self.prepared_cursor_mysql = self.connector_mysql.cursor(prepared=True)
        self.cursor_mysql = self.connector_mysql.cursor(buffered=True)
        self.backup_base_folder = backup_folder
        self.last_backup = f"db_schema--{str(datetime.datetime.now().replace(microsecond=0).timestamp())[:-2]}.db"
        self.last_command = ""
        self.count_operations = 0
        self.count_limit = count_limit          # Quando count arriva a count_limit, fa un backup.

        _table_raw_schema_model = """CREATE TABLE IF NOT EXISTS raw_schema_model
                (
                    domain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    subdomain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    model VARCHAR(50) NOT NULL DEFAULT "Unset",
                    version VARCHAR(10) NOT NULL DEFAULT "0.0.0",
                    attributes JSON,
                    warnings JSON,
                    attributesLog JSON,
                    json_schema JSON,
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (domain, subdomain, model, version)
                );"""
        _table_default_versions = """CREATE TABLE IF NOT EXISTS default_versions
                (
                    domain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    subdomain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    model VARCHAR(50) NOT NULL DEFAULT "Unset",
                    defaultVersion VARCHAR(10) NOT NULL DEFAULT "0.0.0",
                    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (domain, subdomain, model)
                );"""
        try:
            self.cursor_mysql.execute(_table_raw_schema_model)
            self.cursor_mysql.reset()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
        try:
            self.cursor_mysql.execute(_table_default_versions)
            self.cursor_mysql.reset()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))

    def update_default_version(self, model, subdomain=None, domain=None, new_version=None, ignore_if_exists=True):
        if new_version:
            _ignore = ""
            if ignore_if_exists:
                _ignore = "or IGNORE"
            _query = f'INSERT {_ignore} INTO default_versions WHERE model="{model}"'
            if subdomain:
                _query += f' AND subdomain="{subdomain}"'
            if domain:
                _query += f' AND domain="{domain}"'
            _query += f" VALUES({domain}, {subdomain}, {model}, {new_version})"

    def _default_version_procedure(self, model, subdomain, domain, version):
        try:
            a = f"INSERT IGNORE INTO default_versions VALUES ('{domain}', '{subdomain}', '{model}', '{version}', NOW())"
            self.cursor_mysql.execute(a)
            self.cursor_mysql.reset()
        except mysql.connector.Warning as war:
            print("Something went wrong: {}".format(war))
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))

    def get_default_version(self, model, subdomain=None, domain=None):
        _query = f"SELECT defaultVersion FROM default_versions WHERE model='{model}'"
        if subdomain:
            _query += f" AND subdomain='{subdomain}'"
        if domain:
            _query += f" AND domain='{domain}'"
        try:
            self.cursor_mysql.execute(_query)
            res = self.cursor_mysql.fetchone()
            if res is None:
                return None
            else:
                return res[0]
        except mysql.connector.Warning as war:
            print("Something went wrong: {}".format(war))
            return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None

    def _prepare_tuple(self, tuple):
        _domain = tuple[0]
        _subdomain = tuple[1]
        _model = tuple[2]
        _version = tuple[3]
        _atts = json.dumps(tuple[4])
        _errors = json.dumps(tuple[5])
        _atts_log = json.dumps(tuple[6])
        _schema = json.dumps(tuple[7])
        _timestamp = datetime.datetime.now().timestamp()

        return (_domain, _subdomain, _model, _version, _atts, _errors, _atts_log, _schema)

    def _get_solver(self, message, query_res, model, subdomain, domain):
        _default_version = self.get_default_version(model, subdomain, domain)
        if len(message)>0:
            print(message)
        print(f"Found some versions.. Selecting default version for model '{model}' (version {_default_version}).")
        _temp_res = None
        for item in query_res:
            if item[0] == _default_version:
                if _temp_res is None:
                    _temp_res = item[1]
                else:
                    print("Ambiguous.. Specify other parameters, like subdomain or domain.")
                    return None
        return _temp_res

    def add_model(self, tuple):
        try:
            _t = self._prepare_tuple(tuple)
            _domain = _t[0]
            _subdomain = _t[1]
            _model = _t[2]
            _version = _t[3]
            self._default_version_procedure(_model, _subdomain, _domain, _version)
            self.prepared_cursor_mysql.execute(f'INSERT INTO raw_schema_model VALUES (?,?,?,?,?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE model="{_t[2]}"', _t)
            self.prepared_cursor_mysql.reset()
            self.connector_mysql.commit()
            return True, ""
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))

    def generic_query(self, query):
        try:
            self.cursor_mysql.execute(query)
            return self.cursor_mysql.fetchall()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))

    def get_schema(self, model, subdomain=None, domain=None, version=None):
        _query = f'SELECT version, json_schema FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            a = self.cursor_mysql.fetchall()
            if len(a) == 1:
                _dict_str = a[0]
                if _dict_str is not None:
                    res = ast.literal_eval(_dict_str[1])
                    return res
                else:
                    return None
            elif len(a) > 1 and version is None:
                return self._get_solver("", query_res=a, model=model, subdomain=subdomain, domain=domain)
            return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_errors(self, model, subdomain=None, domain=None, version=None, print_res=True):
        _query = f'SELECT version, warnings FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            a = self.cursor_mysql.fetchall()
            if a is not None:
                if len(a) == 1:
                    res = ast.literal_eval(a[0][1])
                    if print_res:
                        _str = f"Errors of model {model}:\n"
                        for error in res:
                            _str += error + "\n"
                        print(_str)
                    return res
                elif len(a) > 1 and version is None:
                    _def_version = self.get_default_version(model, subdomain, domain)
                    print(f"Getting errors of default version (version {_def_version})")
                    _temp_res = None
                    for item in a:
                        if item[0] == _def_version:
                            if _temp_res is None:
                                _temp_res = item[1]
                            else:
                                print("Ambiguous.. Specify other parameters, like subdomain or domain.")
                                return None
                    return None
            else:
                return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_all_versions(self, model, subdomain=None, domain=None):
        _query = f'SELECT version, subdomain, domain FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        try:
            self.cursor_mysql.execute(_query)
            return self.cursor_mysql.fetchall()
        except sqlite3.Error as e:
            print(e.args[0])
            return []

    def get_attributes(self, model, subdomain=None, domain=None, version=None, also_attributes_logs=False):
        _eventually_attr_log = ""
        if also_attributes_logs:
            _eventually_attr_log = ", attributesLog"
        _query = f'SELECT attributes{_eventually_attr_log} FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            a = self.cursor_mysql.fetchall()
            if a is not None:
                if len(a) == 1:
                    res = ast.literal_eval(a[0][0])
                    return res
                elif len(a)>1 and version is None:
                    _def_version = self.get_default_version(model, subdomain, domain)
                    print(f"Found some versions.. Getting attributes of default version (version {_def_version})")
                    _temp_res = None
                    for item in a:
                        if item[0] == _def_version:
                            if _temp_res is None:
                                _temp_res = item[1]
                            else:
                                print("Ambiguous.. Specify other parameters, like subdomain or domain.")
                                return None
                    return None
                return None
            if also_attributes_logs:
                res = ast.literal_eval(a[0])
                attr_log = ast.literal_eval(a[1])
                if len(res) > 0:
                    return [res, attr_log]
                else:
                    return [[], []]
            else:
                res = ast.literal_eval(a[0])
                if len(res) > 0:
                    return [res, []]
                else:
                    return [[], []]
        except sqlite3.Error as e:
            print(e.args[0])
            return [[], []]

    def _update_backup_name(self):
        self.last_backup = f"db_schema--{str(datetime.datetime.now().replace(microsecond=0).timestamp())[:-2]}.db"

    def _backup_path(self):
        return self.backup_base_folder+self.last_backup

    def backup_db(self, actual_command):
        self.count_operations += 1
        if actual_command != self.last_command or self.count_operations > self.count_limit:
            shutil.copyfile(self.db_uri, self._backup_path())
            self._update_backup_name()
            self.count_operations = 0
        self.last_command = actual_command

    def restore_db(self):
        self.connector_sqlite.commit()
        self.connector_sqlite.close()
        os.remove(self.db_uri)
        shutil.copyfile(self.last_backup, self.db_uri)

    def get_unchecked_attrs(self, model, subdomain=None, domain=None, version=None):
        return