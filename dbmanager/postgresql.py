import psycopg2
from dbmanager.base import DBManagerBase


class PsqlManager(DBManagerBase):

    def __init__(self, database="demodb", user="admin", password="admin", host="127.0.0.1", port="5432") -> None:
        super().__init__()
        self.db_config = {
            "database": database,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }
    
    def create_conn(self):
        return psycopg2.connect(**self.db_config)

    def create_tables(self):

        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS COMPANY
            (ID INT PRIMARY KEY     NOT NULL,
            NAME           TEXT    NOT NULL,
            AGE            INT     NOT NULL,
            ADDRESS        CHAR(50),
            SALARY         REAL);''')
        print("Table created successfully")

        self.conn.commit()


def run_test():
    with PsqlManager() as db:
        db.create_tables()


if __name__ == "__main__":
    run_test()


    