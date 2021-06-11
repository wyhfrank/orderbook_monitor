import mysql.connector
from dbmanager.base import DBManagerBase


class MysqlManager(DBManagerBase):
    sql_insert_placeholder = '%s'
    sql_create_orderbook = '''CREATE TABLE IF NOT EXISTS orderbook
        (id              INTEGER    PRIMARY KEY AUTO_INCREMENT,
        symbol           CHAR(20)   NOT NULL,
        best_ask         FLOAT      NOT NULL,
        best_bid         FLOAT      NOT NULL,
        timestamp        INT        NOT NULL,
        exchange         CHAR(20)   NOT NULL
        );'''

    sql_create_depth = '''CREATE TABLE IF NOT EXISTS depth
        (id            INTEGER    PRIMARY KEY AUTO_INCREMENT,
        orderbook_id   INTEGER    NOT NULL,
        side           CHAR(3)    CHECK (side IN  ('ask', 'bid'))   NOT NULL,
        price          FLOAT      NOT NULL,
        amount         FLOAT      NULL DEFAULT NULL,
        FOREIGN KEY (orderbook_id) REFERENCES orderbook (id)
            ON DELETE CASCADE
        );'''

    def __init__(self, database="orderbook_db", user="admin", password="admin", host="localhost", port=3306) -> None:
        super().__init__()
        self.db_config = {
            "database": database,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }
        print(f"Using [mysql]: {host}")
    
    def create_conn(self):
        return mysql.connector.connect(**self.db_config)
    

def run_test():
    config = {
        "user": "admin",
        "host": "",
        "password": "",
        'database': 'orderbook_db',
    }
    with MysqlManager(**config) as db:
        db.create_tables_safe()


if __name__ == "__main__":
    run_test()


    