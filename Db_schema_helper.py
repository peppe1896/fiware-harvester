import sqlite3
import ast

class Db_schema_helper:
    def __init__(self, db_folder):
        self.base_folder = db_folder
        self.connection = sqlite3.connect(self.base_folder + "db_schema.db")
        self.cursor = self.connection.cursor()

        try:
            self.cursor.execute("""CREATE TABLE raw_schema_model
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
                        PRIMARY KEY (Domain, Subdomain, Model, version)
                    );""")
            self.connection.commit()
        except sqlite3.Error as e:
            if "table raw_schema_model already exists" != e.args[0]:
                print(e.args[0])

    def add_tuple(self, tuple):
        try:
            self.cursor.execute('INSERT OR REPLACE INTO raw_schema_model VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple)
            self.connection.commit()
            return True, ""
        except sqlite3.Error as e:
            _msg = "Error in DB: " + e.args[0]
            return False, _msg

    def read_db(self):
        try:
            a = self.cursor.execute('SELECT * FROM raw_schema_model')
            self.connection.commit()
            return a.fetchall()
        except sqlite3.Error as e:
            print("Error in DB: ", e.args[0])
            return None

    def get_select(self,fields:tuple,where:tuple):
        return

    def perform_query(self, query):
        try:
            a = self.cursor.execute(query)
            self.connection.commit()
            return a.fetchone()
        except sqlite3.Error as e:
            print(e)
            return None

    def get_schema(self, model, subdomain=None, domain=None, version=None):
        _query = f'SELECT json_schema FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            a = self.cursor.execute(_query)
            self.connection.commit()
            _dict_str = a.fetchone()
            if _dict_str is not None:
                res = ast.literal_eval(_dict_str[0])
                return res
            else:
                return None
        except sqlite3.Error as e:
            print(e.args[0])
            return None

    def get_errors(self, model, subdomain=None, domain=None, version=None, print_res=True):
        _query = f'SELECT warnings FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        if version:
            _query += f' AND version="{version}"'
        try:
            a = self.cursor.execute(_query)
            self.connection.commit()
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
            print_res(e.args[0])
            return None

    def get_all_versions(self, model, subdomain=None, domain=None):
        _query = f'SELECT json_schema FROM raw_schema_model WHERE model="{model}"'
        if subdomain:
            _query += f' AND subdomain="{subdomain}"'
        if domain:
            _query += f' AND domain="{domain}"'
        try:
            a = self.cursor.execute(_query)
            self.connection.commit()
            res = []
            for json_schema in a:
                s = ast.literal_eval(json_schema[0])
                res.append(s)
            if len(res)>0:
                return res
            else:
                return None
        except sqlite3.Error as e:
            print(e.args[0])
            return None

