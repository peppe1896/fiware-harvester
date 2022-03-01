import sqlite3

class db_schema_helper:
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
                        json_schema TEXT NOT NULL,
                        timestamp INTEGER NOT NULL,
                        version TEXT NOT NULL
                    )""")
            self.connection.commit()
        except sqlite3.Error as e:
            pass

    def add_tuple(self, tuple):
        try:
            self.cursor.execute('INSERT INTO raw_schema_model VALUES (?, ?, ?, ?, ?, ?)', tuple)
            self.connection.commit()
            return True, ""
        except sqlite3.Error as e:
            _msg = "Error in DB:", e.args[0]
            return False, _msg

    def read_db(self):
        try:
            self.cursor.execute('SELECT * FROM raw_schema_model')
        except sqlite3.Error as e:
            print("Error in DB:", e.args[0])


