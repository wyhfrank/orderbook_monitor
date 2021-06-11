import psycopg2
from dbmanager.base import DBManagerBase


class PsqlManager(DBManagerBase):
    sql_insert_placeholder = '%s'    
    sql_create_orderbook = '''CREATE TABLE IF NOT EXISTS orderbook
        (id              SERIAL     PRIMARY KEY,
        symbol           CHAR(20)   NOT NULL,
        best_ask         REAL       NOT NULL,
        best_bid         REAL       NOT NULL,
        timestamp        INT        NOT NULL,
        exchange         CHAR(20)   NOT NULL
        );'''

    sql_create_depth = '''CREATE TABLE IF NOT EXISTS depth
        (id            SERIAL     PRIMARY KEY,
        orderbook_id   INTEGER    NOT NULL,
        side           CHAR(3)    CHECK (side IN  ('ask', 'bid'))   NOT NULL,
        price          REAL       NOT NULL,
        amount         REAL       NULL DEFAULT NULL,
        FOREIGN KEY (orderbook_id) REFERENCES orderbook (id)
            ON DELETE CASCADE
        );'''

    sql_insert_orderbook = "INSERT INTO orderbook ({}) VALUES ({}) RETURNING id;"
    sql_insert_depth =  "INSERT INTO depth ({}) VALUES ({}) RETURNING id;"        

    def __init__(self, database="orderbook_db", user="postgres", password="postgres", host="localhost", port=5432) -> None:
        super().__init__()
        self.db_config = {
            "database": database,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }
        print(f"Using [postgres]: {host}")
    
    def create_conn(self):
        return psycopg2.connect(**self.db_config)
    
    @classmethod
    def get_last_inserted_id(cls, cursor):
        return cursor.fetchone()[0]
    


def run_test():
    with PsqlManager() as db:
        db.create_tables_safe()


if __name__ == "__main__":
    run_test()


    