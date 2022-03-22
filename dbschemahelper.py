import sqlite3
import ast
import datetime
import mysql.connector
import json
import statics


class DbSchemaHelper:
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
        _table_map_id = """CREATE TABLE IF NOT EXISTS map_id
                (
                    model_id INT NOT NULL AUTO_INCREMENT,
                    domain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    subdomain VARCHAR(50) NOT NULL DEFAULT "Unset",
                    model VARCHAR(50) NOT NULL DEFAULT "Unset",
                    version VARCHAR(10) NOT NULL DEFAULT "0.0.0",
                    PRIMARY KEY (model_id)
                );"""
        _table_id_attrs = """CREATE TABLE IF NOT EXISTS id_attrs
                (
                    model_id INT NOT NULL AUTO_INCREMENT,
                    
                    PRIMARY KEY (model_id)
                );"""

        _tables = [_table_raw_schema_model, _table_default_versions, _table_map_id]
        for _table in _tables:
            try:
                self.cursor_mysql.execute(_table)
                self.cursor_mysql.reset()
            except mysql.connector.Error as err:
                print("Something went wrong: {}".format(err))


    def update_default_version(self, model, subdomain, domain, new_version):
        _query = f'UPDATE default_versions SET  defaultVersion="{new_version}" WHERE ' \
                 f'model="{model}" AND subdomain="{subdomain}" AND domain="{domain}"'
        try:
            self.cursor_mysql.execute(_query)
            self.connector_mysql.commit()
            self.cursor_mysql.reset()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.cursor_mysql.reset()
            self.connector_mysql.rollback()

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
        _query = f"SELECT defaultVersion, model, subdomain, domain FROM default_versions WHERE model='{model}'"
        if subdomain:
            _query += f" AND subdomain='{subdomain}'"
        if domain:
            _query += f" AND domain='{domain}'"
        try:
            self.cursor_mysql.execute(_query)
            res = self.cursor_mysql.fetchall()
            if res is None:
                return None
            elif len(res)==1:
                return res[0][0]
            else:
                _temp_schema = None
                for v_m_s_d in res:
                    if _temp_schema is None:
                        _temp_schema = self.get_schema(v_m_s_d[1], v_m_s_d[2], v_m_s_d[3], v_m_s_d[0])
                    else:
                        _compare_schema = self.get_schema(v_m_s_d[1], v_m_s_d[2], v_m_s_d[3], v_m_s_d[0])
                        _equals_to_last_found = statics.json_is_equals(_temp_schema, _compare_schema)
                        if not _equals_to_last_found:
                            print("Ambiguous model. Specify also subdomain and/or domain")
                            _temp_schema = None
                            break
                return _temp_schema
        except mysql.connector.Warning as war:
            print("Something went wrong: {}".format(war))
            return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_id(self, model, subdomain=None, domain=None, version=None):
        _query = f'SELECT * FROM map_id WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            _res = self.cursor_mysql.fetchall()
            if len(_res)==1:
                return _res[0][0]
            else:
                return None
        except mysql.connector.Warning as war:
            print("Something went wrong: {}".format(war))
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))

    def _procedure_add_id(self, model, subdomain, domain, version):
        _maybe_id = self.get_id(model, subdomain, domain, version)
        if not _maybe_id:
            try:
                _query = f"INSERT INTO map_id (model, subdomain, domain, version) VALUES ('{model}', '{subdomain}', '{domain}', '{version}')"
                self.cursor_mysql.execute(_query)
                self.cursor_mysql.reset()
            except mysql.connector.Warning as war:
                print("Something went wrong: {}".format(war))
            except mysql.connector.Error as err:
                print("Something went wrong: {}".format(err))

    def _prepare_tuple(self, _tuple):
        _domain = _tuple[0]
        _subdomain = _tuple[1]
        _model = _tuple[2]
        _version = _tuple[3]
        _atts = json.dumps(_tuple[4])
        _errors = json.dumps(_tuple[5])
        _atts_log = json.dumps(_tuple[6])
        _schema = json.dumps(_tuple[7])
        _timestamp = datetime.datetime.now().timestamp()

        return (_domain, _subdomain, _model, _version, _atts, _errors, _atts_log, _schema)

    def add_model(self, _tuple):
        try:
            _t = self._prepare_tuple(_tuple)
            _domain = _t[0]
            _subdomain = _t[1]
            _model = _t[2]
            _version = _t[3]
            self._default_version_procedure(_model, _subdomain, _domain, _version)
            self._procedure_add_id(_model, _subdomain, _domain, _version)
            self.prepared_cursor_mysql.execute(f'INSERT INTO raw_schema_model VALUES (?,?,?,?,?,?,?,?,NOW()) ON DUPLICATE KEY UPDATE model="{_t[2]}"', _t)
            self.connector_mysql.commit()
            self.prepared_cursor_mysql.reset()
            self.connector_mysql.commit()
            return True, ""
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.connector_mysql.rollback()

    def generic_query(self, query):
        try:
            self.cursor_mysql.execute(query)
            return self.cursor_mysql.fetchall()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))

    def get_schema(self, model, subdomain=None, domain=None, version=None):
        _query = f'SELECT version, json_schema, subdomain, domain FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            query_res = self.cursor_mysql.fetchall()
            if len(query_res) == 1:
                _dict_str = query_res[0]
                if _dict_str is not None:
                    _string = _dict_str[1]
                    res = json.loads(_string)
                    #res = ast.literal_eval(_string)
                    return res
                else:
                    return None
            elif len(query_res) > 1 and version is None:
                _default_version = self.get_default_version(model, subdomain, domain)
                if _default_version is None:
                    return None
                print(f"Found some versions.. Selecting default version for model '{model}' (version {_default_version}).")
                _temp_res = None
                for item in query_res:
                    if item[0] == _default_version:
                        if _temp_res is None:
                            _temp_res = item[1]
                        elif not statics.json_is_equals(_temp_res, item[1]):
                            print("Ambiguous.. Specify other parameters, like subdomain or domain.")
                            return None
                        else:
                            print(f"Same version of schema in different keys 'domain'({item[3]}) and 'subdomain'({item[2]}).")
                return _temp_res
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
                    if _def_version is None:
                        return None
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
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_attributes(self, model, subdomain=None, domain=None, version=None, also_attributes_logs=False):
        _eventually_attr_log = ""
        if also_attributes_logs:
            _eventually_attr_log = ", attributesLog"
        _query = f'SELECT version, attributes{_eventually_attr_log} FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            query_res = self.cursor_mysql.fetchall()
            if query_res is not None:
                if len(query_res) == 1:
                    res = ast.literal_eval(query_res[0][1])
                    if also_attributes_logs:
                        _a_log = ast.literal_eval(query_res[0][2])
                        return [res, _a_log]
                    return [res, []]
                elif len(query_res) > 1 and version is None:
                    _default_version = self.get_default_version(model, subdomain, domain)
                    if _default_version is None:
                        return None
                    print(f"Found some versions.. Selecting default version for model '{model}' (version {_default_version}).")
                    _temp_atts = None
                    _temp_atts_log = None
                    for item in query_res:
                        if item[0] == _default_version:
                            if _temp_atts is None:
                                _temp_atts = item[0]
                                if also_attributes_logs:
                                    _temp_atts_log = item[1]
                            else:
                                print("Ambiguous.. Specify other parameters, like subdomain or domain.")
                                _temp_atts = None
                                _temp_atts_log = None
                                break
                    return [_temp_atts, _temp_atts_log]
                else:
                    return [None, None]
            return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_unchecked_attrs(self, model=None, subdomain=None, domain=None, version=None):
        _query = f'SELECT model, subdomain, domain, version, attributes->"$.*.value_name" FROM raw_schema_model WHERE attributes->>"$.*.checked"=False'
        if model:
            _query += f' AND model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        _query += " ORDER BY model"
        try:
            self.cursor_mysql.execute(_query)
            q_res = self.cursor_mysql.fetchall()
            if not model and not subdomain and not domain and not version:
                _temp = []
                for _tuple in q_res:
                    _atts = json.loads(_tuple[4])
                    _tple = (_tuple[0], _tuple[1], _tuple[2], _tuple[3], _atts)
                    _temp.append(_tple)
                return _temp    # Seleziona tutti i modelli
            if model and subdomain and domain and version:
                return ast.literal_eval(q_res[0][4])
            if q_res is not None:
                if len(q_res) == 1:
                    return ast.literal_eval(q_res[0][4])
                elif len(q_res) > 1:
                    _def_version = self.get_default_version(model, subdomain, domain)
                    if _def_version is None:
                        return None
                    _temp_res = None
                    for item in q_res:
                        if item[3] == _def_version:
                            if _temp_res is None:
                                _temp_res = item[4]
                            else:
                                _temp_res = None
                    return _temp_res
            else:
                return None
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_attribute(self, attribute_name, model=None, subdomain=None, domain=None, version=None, onlyChecked_True_False=None):
        _query = f"""select model, subdomain, domain, version, attributes->"$.{attribute_name}" from raw_schema_model where json_contains(attributes->"$.*.value_name", '"{attribute_name}"', "$" )"""
        if model:
            _query += f' AND model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        if onlyChecked_True_False:
            _query += f' AND attributes->>"$.{attribute_name}.checked"={onlyChecked_True_False}'
        _query_res = self.generic_query(_query)
        _res = []
        for item in _query_res:
            _mdl = item[0]
            _sbd = item[1]
            _dmn = item[2]
            _vrs = item[3]
            _att = json.loads(item[4])
            _res.append((_mdl, _sbd, _dmn, _vrs, _att))
        return _res

    def update_checked(self, model, subdomain, domain, version, attribute_name, checked_value="False"):
        _query = f'UPDATE raw_schema_model ' \
                 f'SET attributes=JSON_REPLACE(attributes, "$.{attribute_name}.checked", "{checked_value}") ' \
                 f'WHERE model="{model}" AND subdomain="{subdomain}" AND domain="{domain}" AND version="{version}"'
        try:
            self.cursor_mysql.execute(_query)
            self.connector_mysql.commit()
            self.cursor_mysql.reset()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            self.connector_mysql.rollback()
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            self.connector_mysql.rollback()
            return None