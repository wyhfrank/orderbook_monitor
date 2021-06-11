import os
from dbmanager import PsqlManager, MysqlManager, SqliteManager

def get_db_manager():
    engine = os.environ.get("USE_DB", "sqlite")
    engine = engine.lower()

    if engine == "mysql":
        return get_mysql()
    elif engine in ["postgres", "psql", "postgresql"]:
        return get_postgres()
    elif engine == "sqlite":
        return get_sqlite()
    
    print(f"DB manager type not supported: {engine}")

def get_mysql():
    db_config = {
        "host":  os.environ.get("MYSQL_HOST", "localhost"),
        "user":  os.environ.get("MYSQL_USER", "admin"),
        "password":  os.environ.get("MYSQL_PASSWORD", "password"),
        "port":  os.environ.get("MYSQL_PORT", 3306),
        "database": os.environ.get("MYSQL_DB_NAME", "orderbook_db"),
        
    }
    return MysqlManager(**db_config)
    
def get_postgres():
    db_config = {
        "host":  os.environ.get("POSTGRES_HOST", "localhost"),
        "user":  os.environ.get("POSTGRES_USER", "admin"),
        "password":  os.environ.get("POSTGRES_PASSWORD", "password"),
        "port":  os.environ.get("POSTGRES_PORT", 5432),
        "database": os.environ.get("POSTGRES_DB_NAME", "orderbook_db"),
    }
    return PsqlManager(**db_config)


def get_sqlite():
    db_config = {
        "file": os.environ.get("SQLITE_DB_FILE", "history.db"),
        "path": os.environ.get("SQLITE_DB_PATH", "data/sqlite_data"),
    }
    return SqliteManager(**db_config)
