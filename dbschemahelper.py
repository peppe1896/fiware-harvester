import ast
import datetime
import mysql.connector
import json
import statics


class DbSchemaHelper:
    def __init__(self, db_folder, backup_folder, count_limit=10):
        self.base_folder = db_folder
        self.backup_base_folder = backup_folder
        self.connector_mysql = mysql.connector.connect(
            host="localhost",
            user="peppe1896",
            password="Password1!",
            database="smartdatamodels"
        )
        self.prepared_cursor_mysql = self.connector_mysql.cursor(prepared=True)
        self.cursor_mysql = self.connector_mysql.cursor(buffered=True)
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
        _table_id_attrs = """CREATE VIEW attrs AS 
                SELECT model, domain, subdomain, version, value_name, value_type, data_type, value_unit, 
                    healthiness_criteria, healthiness_value, editable, checked, raw_attribute 
                FROM raw_schema_model, json_table(
                    raw_schema_model.attributes, '$.*' 
                        COLUMNS( 
                            value_name VARCHAR(50) path '$.value_name',
                            data_type VARCHAR(50) path '$.data_type',
                            value_type VARCHAR(50) path '$.value_type',
                            value_unit VARCHAR(50) path '$.value_unit',  
                            healthiness_criteria VARCHAR(50) path '$.healthiness_criteria',  
                            healthiness_value VARCHAR(50) path '$.healthiness_value',  
                            editable VARCHAR(50) path '$.editable',  
                            checked VARCHAR(30) path '$.checked',  
                            raw_attribute JSON path '$.raw_attribute')) as t;
                """
        _table_rules = """CREATE TABLE IF NOT EXISTS`EXT_values_rules` (
                      `Name` varchar(40) NOT NULL,
                      `If_statement` text DEFAULT NULL,
                      `Then_statement` text DEFAULT NULL,
                      `Organization` varchar(40) DEFAULT NULL,
                      `Timestamp` datetime NOT NULL,
                      `mode` varchar(45) DEFAULT NULL,
                      `contextbroker` varchar(45) DEFAULT NULL,
                      `service` varchar(25) DEFAULT NULL,
                      `servicePath` varchar(96) DEFAULT NULL,
                      PRIMARY KEY (`Timestamp`)
                );"""

        _tables = [
            _table_raw_schema_model,
            _table_default_versions,
            _table_map_id,
            _table_rules
        ]
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
            elif len(res) == 1:
                return res[0][0]
            else:
                if self.check_same_modelsName_same_schema(model):
                    return res[0][0]
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

    def get_model_schema(self, model, subdomain=None, domain=None, version=None):
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

    def get_attributes(self, model=None, subdomain=None, domain=None, version=None, onlyChecked="", also_attributes_logs=False, excludeType=True, excludedAttr=[]):
        _eventually_attr_log = ""
        _attributes = "rsm.attributes"
        if also_attributes_logs:
            _eventually_attr_log = ", rsm.attributesLog"
        _query = f'SELECT {_attributes}{_eventually_attr_log}, rsm.model, rsm.subdomain, rsm.domain, rsm.version FROM raw_schema_model as rsm, attrs as t WHERE rsm.version!=" "'
        if model:
            _query += f' AND rsm.model="{model}"'
        if subdomain:
            _query += f' AND rsm.subdomain="{subdomain}"'
        if domain:
            _query += f' AND rsm.domain="{domain}"'
        if version:
            _query += f' AND rsm.version="{version}"'
        if onlyChecked:
            _query += f' AND JSON_CONTAINS(rsm.attributes->"$.*.checked",\'"{onlyChecked}"\',"$")'
        try:
            self.cursor_mysql.execute(_query)
            query_res = self.cursor_mysql.fetchall()
            if len(excludedAttr) > 0 or onlyChecked:
                if len(excludedAttr) > 0:
                    _num_res = len(query_res)
                    _iterator = 0
                    while _iterator < _num_res:
                        _tuple = query_res.pop(0)
                        _attrs = json.loads(_tuple[1])
                        for _attr in excludedAttr:
                            if _attr in _attrs.keys():
                                _attrs.pop(_attr)
                        _new_tuple = list(_tuple)
                        _new_tuple[1] = json.dumps(_attrs)
                        _new_tuple = tuple(_new_tuple)
                        query_res.append(_new_tuple)
                        _iterator += 1
                if onlyChecked:
                    _num_res = len(query_res)
                    _iterator = 0
                    while _iterator < _num_res:
                        _tuple = query_res.pop(0)
                        _attrs = json.loads(_tuple[0])
                        _new_attrs = {}
                        _attrs_keys = list(_attrs.keys())
                        for _attr_key in _attrs_keys:
                            _attr = _attrs.pop(_attr_key)
                            if _attr["checked"] == onlyChecked:
                                _new_attrs[_attr_key] = _attr
                        _new_tuple = list(_tuple)
                        _new_tuple[0] = json.dumps(_new_attrs)
                        _new_tuple = tuple(_new_tuple)
                        query_res.append(_new_tuple)
                        _iterator += 1
            if len(query_res) == 1:
                _tmp = list(query_res.pop(0))
                res = json.loads(_tmp[0])
                _tmp[0] = res
                if also_attributes_logs:
                    _a_log = json.loads(_tmp[1])
                    _tmp[1] = _a_log
                return [tuple(_tmp)]
            elif len(query_res) == 0:
                if also_attributes_logs:
                    return {}, {}
                return {}
            _temp = []
            while len(query_res) > 0:
                _tuple = query_res.pop(0)
                _tuple = list(_tuple)
                _tuple[0] = json.loads(_tuple[0])
                if also_attributes_logs:
                    _tuple[1] = json.loads(_tuple[1])
                _temp.append(tuple(_tuple))
            return _temp
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None

    def get_unchecked_attrs(self, model=None, subdomain=None, domain=None, version=None):
        _query = f'SELECT model, subdomain, domain, version, attributes->"$.*.value_name" FROM raw_schema_model WHERE json_contains(attributes->"$.*.checked",\'"False"\',"$")'
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
            _query += f' AND attributes->>"$.{attribute_name}.checked"="{onlyChecked_True_False}"'
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

    def get_sbdom_dom(self, model, version=None):
        _query = f"SELECT model, subdomain, domain, version FROM raw_schema_model WHERE model='{model}'"
        if version:
            _query += f" AND version='{version}'"
        _query_res = self.generic_query(_query)
        if len(_query_res) == 1:
            return _query_res[0][1], _query_res[0][2], _query_res[0][3]
        else:
            print("Some version of this model has been found in database")
            self.check_same_modelsName_same_schema(model)

    # Controlla se tutti i modelli con quel nome sono uguali tra loro.
    # Questo puà andare bene a questo livello, ma se venissero rilasciate nuove versioni di questi modelli, questo
    # dovrebbe non funzionare più TODO Leggi qui
    # Potrebbe essere vincolato usando anche la versione: modelli con lo stesso nome e stessa versione, sono identici?
    # Possibile funzioni così.
    def check_same_modelsName_same_schema(self, model, version=None):
        _versions = self.get_all_versions(model)
        _same_schemas = True
        if len(_versions) > 1:
            _itr = 0
            while _itr < len(_versions) - 1:
                # Each tuple is (version, subdomain, domain)
                _sch_outside = _versions[_itr]
                _current_schema = self.get_model_schema(model, _sch_outside[1], _sch_outside[2], _sch_outside[0])
                _iterator = _itr
                _itr += 1
                while _iterator < len(_versions):
                    _sch_inside = _versions[_iterator]
                    _schema = self.get_model_schema(model, _sch_inside[1], _sch_inside[2], _sch_inside[0])
                    if not statics.json_is_equals(_current_schema, _schema):
                        _same_schemas = False
                    _iterator += 1
            if _same_schemas:
                print(f"All of this schemas are equals. Model: '{model}'")
            else:
                print(f"Different schemas with same model name. Model: '{model}'")
        return _same_schemas

    def update_json_attribute(self, model, subdomain, domain, version, attribute_name, field="checked", value_to_set="False"):
        _query = f'UPDATE raw_schema_model ' \
                 f'SET attributes=JSON_REPLACE(attributes, "$.{attribute_name}.{field}", "{value_to_set}") ' \
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

    def count_attributes(self, attribute, group_by=""):
        if not group_by:
            group_by = "model"
        _query = f'select {group_by}, count(*) from raw_schema_model ' \
                 f'where json_contains(attributes->"$.*.value_name", \'"{attribute}"\', "$" ) ' \
                 f'group by {group_by}'
        res = self.generic_query(_query)
        return res

    def add_rule(self, rule, multitenancy=None):
        _name = rule[0]
        _ifs = json.dumps(rule[1])
        _thens = json.dumps(rule[2])
        _org = rule[3]
        _cb = rule[4]
        _t = (_name, _ifs, _thens, _org, _cb)
        if multitenancy:
            _srv = rule[5]
            _service_path = rule[6]
            _t = (_name, _ifs, _thens, _org, _cb, _srv, _service_path)
        try:
            if multitenancy:
                self.prepared_cursor_mysql.execute(f'INSERT INTO EXT_values_rules VALUES (?,?,?,?,NOW(),"1",?,?,?)', _t)
            else:
                self.prepared_cursor_mysql.execute(f'INSERT INTO EXT_values_rules VALUES (?,?,?,?,NOW(),"1", ?, Null, Null)', _t)
            self.connector_mysql.commit()
            self.prepared_cursor_mysql.reset()
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            return None
        except mysql.connector.Warning as err:
            print("Something went wrong: {}".format(err))
            return None