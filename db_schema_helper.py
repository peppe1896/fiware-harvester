import sqlite3
import ast
import shutil
import datetime
import os
import mysql.connector as connector


class Db_schema_helper:
    def __init__(self, db_folder, backup_folder, count_limit=10):
        self.base_folder = db_folder
        self.db_uri = self.base_folder + "db_schema.db"
        self.connector_sqlite = sqlite3.connect(self.db_uri)
        self.cursor_sqlite = self.connector_sqlite.cursor()
        self.connector_mysql = connector.connect(
            host="localhost",
            user="root",
            password="my_secret_password"
        )
        self.backup_base_folder = backup_folder
        self.last_backup = f"db_schema--{str(datetime.datetime.now().replace(microsecond=0).timestamp())[:-2]}.db"
        self.last_command = ""
        self.count_operations = 0
        self.count_limit = count_limit          # Quando count arriva a count_limit, fa un backup.

        try:
            self.cursor_sqlite.execute("""CREATE TABLE raw_schema_model
                    (
                        domain TEXT NOT NULL,
                        subdomain TEXT NOT NULL,
                        model TEXT NOT NULL,
                        version TEXT NOT NULL,
                        attributes TEXT NOT NULL,
                        warnings TEXT NOT NULL,
                        attributesLog TEXT NOT NULL,
                        json_schema TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        PRIMARY KEY (domain, subdomain, model, version)
                    );""")
            self.cursor_sqlite.execute("""CREATE TABLE default_versions
                    (
                        domain TEXT NOT NULL,
                        subdomain TEXT NOT NULL,
                        model TEXT NOT NULL,
                        defaultVersion TEXT NOT NULL,
                        PRIMARY KEY (domain, subdomain, model)
                    );""")
            self.connector_sqlite.commit()
            self.backup_db("create_db")
        except sqlite3.Error as e:
            if "table raw_schema_model already exists" != e.args[0]:
                print(e.args[0])
                self.restore_db()
            elif "table default_versions already exists" != e.args[0]:
                a = None

    def update_default_version(self, model, subdomain=None, domain=None, new_version=None, ignore_if_exists=True):
        if new_version:
            self.backup_db("update_default_version")
            _ignore = ""
            if ignore_if_exists:
                _ignore = "or IGNORE"
            _query = f'INSERT {_ignore} INTO default_versions WHERE model="{model}"'
            if subdomain:
                _query += f' AND subdomain="{subdomain}"'
            if domain:
                _query += f' AND domain="{domain}"'
            _query += f" VALUES({domain}, {subdomain}, {model}, {new_version})"

    def add_model(self, tuple):
        self.backup_db("add_model")
        try:
            _domain = tuple[0]
            _subdomain = tuple[1]
            _model = tuple[2]
            _version = tuple[3]
            self.cursor_sqlite.execute('INSERT OR REPLACE INTO raw_schema_model VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple)
            self.connector_sqlite.commit()
            return True, ""
        except sqlite3.Error as e:
            _msg = "Error in DB: " + e.args[0]
            self.restore_db()
            return False, _msg

    def generic_query(self, query):
        self.backup_db("generic_query")
        self.last_command = "generic_query"
        try:
            a = self.cursor_sqlite.execute(query)
            self.connector_sqlite.commit()
            return a.fetchone()
        except sqlite3.Error as e:
            print(e)
            self.restore_db()
            return None

    def get_schema(self, model, subdomain=None, domain=None, version=None):
        self.backup_db("get_schema")
        _query = f'SELECT json_schema FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            a = self.cursor_sqlite.execute(_query)
            self.connector_sqlite.commit()
            _dict_str = a.fetchone()
            if _dict_str is not None:
                res = ast.literal_eval(_dict_str[0])
                return res
            else:
                return None
        except sqlite3.Error as e:
            print(e.args[0])
            self.restore_db()
            return None

    def get_errors(self, model, subdomain=None, domain=None, version=None, print_res=True):
        self.backup_db("get_errors")
        _query = f'SELECT warnings FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            a = self.cursor_sqlite.execute(_query)
            self.connector_sqlite.commit()
            _list_str = a.fetchone()
            if _list_str is not None:
                res = ast.literal_eval(_list_str[0])
                if print_res:
                    _str = f"Errors of model {model}"
                    for error in res:
                        _str += error + "\n"
                    print(_str)
                return res
            else:
                return None
        except sqlite3.Error as e:
            print(e.args[0])
            self.restore_db()
            return None

    def get_all_versions_schemas(self, model, subdomain=None, domain=None):
        self.backup_db("get_all_versions_schemas")
        _query = f'SELECT json_schema FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        try:
            a = self.cursor_sqlite.execute(_query)
            self.connector_sqlite.commit()
            res = []
            for json_schema in a:
                s = ast.literal_eval(json_schema[0])
                res.append(s)
            if len(res) > 0:
                return res
            else:
                return []
        except sqlite3.Error as e:
            print(e.args[0])
            self.restore_db()
            return []

    def get_all_versions(self, model, subdomain=None, domain=None):
        self.backup_db("get_all_versions")
        _query = f'SELECT version FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        try:
            a = self.cursor_sqlite.execute(_query)
            self.connector_sqlite.commit()
            res = []
            for _version in a:
                res.append(_version)
            if len(res) > 0:
                return res
            else:
                return []
        except sqlite3.Error as e:
            print(e.args[0])
            self.restore_db()
            return []

    def get_attributes(self, model, subdomain=None, domain=None, version=None, also_attributes_logs=False):
        self.backup_db("get_attributes")
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
            a = self.cursor_sqlite.execute(_query)
            self.connector_sqlite.commit()
            a = a.fetchone()
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
            self.restore_db()
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