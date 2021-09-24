from django.core.management.base import BaseCommand
from config.celery import debug_task
from datasource.rkd import RkdStream,bulk_update_rtdb
import time
from config.celery import app

def callback(message):
    print(message)

class Command(BaseCommand):
    def handle(self, *args, **options):
        # bulk_update_rtdb.apply_async(args=('args',),queue='broadcaster')
        # rkd = RkdStream()
        # rkd.chanels = 'market'
        # proc = rkd.thread_stream_quote()
        # proc.daemon = True
        # proc.start()
        
        # while True:
        #     print('ok')
        #     time.sleep(5)
        # app.send_task("djangomodule.aoa.apa", kwargs={
        #               "payload": "222"}, queue='javascript')
        
        # while True:
        #     time.sleep(2)
        #     if r.status ==""
        #     print(r.status)
        # r.get(on_message=callback, propagate=False)

