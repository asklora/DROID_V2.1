from config.celery import app


@app.task
def tasktest():
    print('task queue pc4')