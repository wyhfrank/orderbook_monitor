import os
import sqlite3
from dbmanager.base import DBManagerBase
       

class SqlManager(DBManagerBase):
    sql_create_orderbook = '''CREATE TABLE IF NOT EXISTS orderbook
        (id INTEGER PRIMARY KEY     AUTOINCREMENT,
        symbol           CHAR(20)   NOT NULL,
        best_ask         REAL       NOT NULL,
        best_bid         REAL       NOT NULL,
        timestamp        INT        NOT NULL,
        exchange         CHAR(20)   NOT NULL
        );'''

    sql_create_depth = '''CREATE TABLE IF NOT EXISTS depth
        (id INTEGER PRIMARY KEY   AUTOINCREMENT,
        orderbook_id   INTEGER    NOT NULL,
        side           CHAR(3)    CHECK (side IN  ('ask', 'bid'))   NOT NULL,
        price          REAL       NOT NULL,
        amount         REAL       NULL DEFAULT NULL,
        FOREIGN KEY (orderbook_id) REFERENCES orderbook (id)
            ON DELETE CASCADE
        );'''
        
    def __init__(self, file='history.db', db_path='.') -> None:
        super().__init__()
        self.file = os.path.join(db_path, file)

    def create_conn(self):
        return sqlite3.connect(self.file)



###################################################
def test_db():
    db = SqlManager()
    db.connect()
    db.create_tables_safe()

    records = [
        {
            'symbol': 'btc_jpy',
            'best_ask': 1000,
            'best_bid': 900,
            'timestamp': 1999,
            'exchange': 'bitbank',
            'depth': {
                'ask': [
                    {'price': 1000, 'amount': 1},
                    {'price': 1001, 'amount': 3},
                ],
                'bid': [
                    {'price': 900, 'amount': 1},
                    {'price': 899, 'amount': 2},
                ]
            }
        },
        {
            'symbol': 'btc_jpy',
            'best_ask': 1000,
            'best_bid': 900,
            'timestamp': 1999,
            'exchange': 'decurrent',
            'depth': {
                'ask': [
                    {'price': 1000, 'amount': 1},
                    {'price': 1001, 'amount': 3},
                ],
                'bid': [
                    {'price': 900, 'amount': 1},
                    {'price': 899, 'amount': 2},
                ]
            }
        },
    ]

    db.insert_records(records=records)
    db.close()


if __name__ == "__main__":
    test_db()
