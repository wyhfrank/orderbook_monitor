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
