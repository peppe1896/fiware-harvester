import sqlite3

class Db_schema_helper:
    def __init__(self, db_folder):
        self.base_folder = db_folder
        self.connection = sqlite3.connect(self.base_folder + "db_schema.db")
        self.cursor = self.connection.cursor()

        try:
            self.cursor.execute("""CREATE TABLE raw_schema_model
                    (
                        Domain TEXT NOT NULL,
                        Subdomain TEXT NOT NULL,
                        Model TEXT NOT NULL,
                        version TEXT NOT NULL,
                        attributes TEXT NOT NULL,
                        warnings TEXT NOT NULL,
                        json_schema TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        PRIMARY KEY (Domain, Subdomain, Model, version)
                    );""")
            self.connection.commit()
        except sqlite3.Error as e:
            print(e.args[0])

    def add_tuple(self, tuple):
        try:
            self.cursor.execute('INSERT OR REPLACE INTO raw_schema_model VALUES (?, ?, ?, ?, ?, ?, ?, ?)', tuple)
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
            return a.fetchall()
        except sqlite3.Error as e:
            print(e)
            return None