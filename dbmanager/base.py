class DBManagerBase:
    sql_create_orderbook = ''
    sql_create_depth = ''
    sql_insert_placeholder = '?'
    sql_insert_orderbook = "INSERT INTO orderbook ({}) VALUES ({});"
    sql_insert_depth =  "INSERT INTO depth ({}) VALUES ({});"

    orderbook_columns = [
        'symbol',
        'best_ask',
        'best_bid',
        'timestamp',
        'exchange',
    ] 
    depth_columns = [
        'orderbook_id',
        'side',
        'price',
        'amount',
    ]        

    def __init__(self) -> None:
        self.conn = None
        self.o_sql = self.construct_insert_sql(self.sql_insert_orderbook, self.orderbook_columns)

        self.depth_key = 'depth'
        self.d_sql = self.construct_insert_sql(self.sql_insert_depth, self.depth_columns)        
    
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

    def create_tables_safe(self):
        c = self.conn.cursor()
        c.execute(self.sql_create_orderbook)
        c.execute(self.sql_create_depth)
        self.conn.commit()
        print("Table created successfully")

    @classmethod
    def get_last_inserted_id(cls, cursor):
        return cursor.lastrowid

    def insert_records(self, records):

        c = self.conn.cursor()
        for r in records:
            orderbook_data = []
            for oc in self.orderbook_columns:
                orderbook_data.append(r[oc])
            c.execute(self.o_sql, orderbook_data)

            # Write depth data
            orderbook_id = self.get_last_inserted_id(c)
            depth_data = r[self.depth_key]
            for side in ('ask', 'bid'):
                depth_list = depth_data[side]
                for dr in depth_list:
                    c.execute(self.d_sql, (orderbook_id, side, dr['price'], dr['amount']))
        
        self.conn.commit()

    @classmethod
    def construct_insert_sql(cls, sql_template, cols):
        sql = sql_template.format(",".join(cols), ",".join([cls.sql_insert_placeholder] * len(cols)))
        return sql
