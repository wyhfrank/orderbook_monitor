import os
import sqlite3

class DBManagerBase:
    def __init__(self) -> None:
        self.conn = None
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, type, value, trace):
        self.close()

    def connect(self):
        self.conn = self.create_conn()
        print("Opened database successfully")
    
    def create_conn(self):
        raise NotImplementedError

    def close(self):
        self.conn.close()
        print("Database connection closed")        


class SqlManager(DBManagerBase):
    def __init__(self, file='history.db', db_path='.') -> None:
        super().__init__()
        self.file = os.path.join(db_path, file)

    def create_conn(self):
        return sqlite3.connect(self.file)

    def create_tables_safe(self):
        c = self.conn.cursor()
        sql_orderbook = '''CREATE TABLE IF NOT EXISTS orderbook
            (id INTEGER PRIMARY KEY     AUTOINCREMENT,
            symbol           CHAR(20)   NOT NULL,
            best_ask         REAL       NOT NULL,
            best_bid         REAL       NOT NULL,
            timestamp        INT        NOT NULL,
            exchange         CHAR(20)   NOT NULL
            );'''

        sql_depth = '''CREATE TABLE IF NOT EXISTS depth
            (id INTEGER PRIMARY KEY   AUTOINCREMENT,
            orderbook_id   INTEGER    NOT NULL,
            side           CHAR(3)    CHECK (side IN  ('ask', 'bid'))   NOT NULL,
            price          REAL       NOT NULL,
            amount         REAL       NULL DEFAULT NULL,
            FOREIGN KEY (orderbook_id) REFERENCES orderbook (id)
              ON DELETE CASCADE
            );'''
        
        c.execute(sql_orderbook)
        c.execute(sql_depth)
        self.conn.commit()
        print("Table created successfully")
    
    def insert_records(self, records):
        orderbook_columns = [
            'symbol',
            'best_ask',
            'best_bid',
            'timestamp',
            'exchange',
        ]
        o_sql = "INSERT INTO orderbook ({}) VALUES (?, ?, ?, ?, ?)".format(",".join(orderbook_columns))

        depth_key = 'depth'
        depth_columns = [
            'orderbook_id',
            'side',
            'price',
            'amount',
        ]
        d_sql = "INSERT INTO depth ({}) VALUES (?, ?, ?, ?)".format(",".join(depth_columns))
        
        c = self.conn.cursor()

        for r in records:
            orderbook_data = []
            for oc in orderbook_columns:
                orderbook_data.append(r[oc])
            c.execute(o_sql, orderbook_data)

            # Write depth data
            orderbook_id = c.lastrowid
            depth_data = r[depth_key]
            for side in ('ask', 'bid'):
                depth_list = depth_data[side]
                for dr in depth_list:
                    c.execute(d_sql, (orderbook_id, side, dr['price'], dr['amount']))
        
        self.conn.commit()


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
