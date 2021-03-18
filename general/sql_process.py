import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from core.djangomodule.network.cloud import DroidDb


load_dotenv()

db = DroidDb()
debug=os.getenv("DROID_DEBUG")
if debug:
    read_endpoint,write_endpoint,port = db.test_url
else:
    read_endpoint,write_endpoint,port = db.prod_url
db_read = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+read_endpoint+":"+str(port)+"/"+os.getenv("DBUSER")
db_write = "postgres://"+os.getenv("DBNAME")+":"+os.getenv("DBPASSWORD")+"@"+write_endpoint+":"+str(port)+"/"+os.getenv("DBUSER")

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