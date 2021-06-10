import os
from dbmanager import PsqlManager

def get_db_manager():
    db_config = {
        "host":  os.environ.get("POSTGRES_HOST", "localhost")
    }

    return PsqlManager(**db_config)