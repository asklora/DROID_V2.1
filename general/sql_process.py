import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from core.djangomodule.network.cloud import DroidDb
from environs import Env
from global_vars import DB_URL_ALIBABA
env = Env()
load_dotenv()
db = DroidDb()

debug=env.bool("DROID_DEBUG")
if debug:
    read_endpoint, write_endpoint, port = db.test_url
else:
    read_endpoint, write_endpoint, port = db.prod_url
db_read = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+read_endpoint+":"+str(port)+"/"+os.getenv("DBUSER")
db_write = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+write_endpoint+":"+str(port)+"/"+os.getenv("DBUSER")
alibaba_db_url = DB_URL_ALIBABA

def get_debug_url():
    debug_read_endpoint, debug_write_endpoint, debug_port = db.test_url
    db_debug_write = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+debug_write_endpoint+":"+str(debug_port)+"/"+os.getenv("DBUSER")
    return db_debug_write
    
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