from django.core.management.base import BaseCommand
from config.celery import debug_task
from datasource.rkd import RkdStream
import time

class Command(BaseCommand):
    def handle(self, *args, **options):
        # debug_task.apply_async(queue='broadcaster')
        rkd = RkdStream()
        rkd.chanels = 'market'
        proc = rkd.thread_stream_quote()
        proc.daemon = True
        proc.start()
        
        while True:
            print('ok')
            time.sleep(5)
        # app.send_task("djangomodule.aoa.apa", kwargs={
        #               "payload": "222"}, queue='javascript')
