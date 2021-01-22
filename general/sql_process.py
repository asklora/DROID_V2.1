import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
db_read = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTREAD")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")
db_write = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+os.getenv("DBHOSTWRITE")+":"+os.getenv("DBPORT")+"/"+os.getenv("DBUSER")

def do_function(func):
    engine = create_engine(db_write)
    print("create connections to db")
    connection = engine.raw_connection()
    cursor = connection.cursor()
    try:
        print(f"trigger functions {func}")
        cursor.callproc(func)
        cursor.close()
        connection.commit()
        print(f"functions {func} succeed")
    except Exception as e:
        print(f"error: {e}")
    finally:
        print("close connections")
        connection.close()
        print("COMPLETE")
    engine.dispose()