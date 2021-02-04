from config.celery import app

from ingestion.master_data import interest_update



@app.task
def tasktest():
    interest_update()
    return "success"