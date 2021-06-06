import os
import sqlite3

class DBManager:
    def __init__(self, file='history.db', db_path='.') -> None:
        self.conn = None
        self.file = os.path.join(db_path, file)

    def connect(self):
        self.conn = sqlite3.connect(self.file)
        print("Opened database successfully")

    def create_db(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS orderbook
            (id INTEGER PRIMARY KEY     AUTOINCREMENT,
            symbol           CHAR(20)   NOT NULL,
            best_ask           REAL    NOT NULL,
            best_bid           REAL    NOT NULL,
            timestamp            INT     NOT NULL,
            exchange        CHAR(20)    NOT NULL
            );''')
        self.conn.commit()
        print("Table created successfully")
    
    def insert_records(self, records):
        columns = [
            'symbol',
            'best_ask',
            'best_bid',
            'timestamp',
            'exchange',
        ]
        sql = "INSERT INTO orderbook ({}) VALUES (?, ?, ?, ?, ?)".format(",".join(columns))
        
        record_list = []
        for r in records:
            row = []
            for c in columns:
                row.append(r[c])
            record_list.append(row)
        
        c = self.conn.cursor()
        c.executemany(sql, record_list)
        self.conn.commit()

    def close(self):
        self.conn.close()
