from django.core.management.base import BaseCommand
import time
from config.celery import app,_RPC,app_publish
from django.conf import settings
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
        # payload ={
        #     "type":"function",
        #     "module":"djangomodule.crudlib.notification.send_notif",
        #     "payload":{
        #     "username":"babebo",
        #     "title":"notif",
        #     "body":"hallo boy"
        #     }
        # }
        # task = app.send_task("config.celery.listener", args=(payload,), queue=settings.ASKLORA_QUEUE,backend=_RPC)
        # print(task.get())
        
        # while True:
        #     time.sleep(2)
        #     print(task.backend)
        # r.get(on_message=callback, propagate=False)
        # r=app_publish.delay()
        # print(r.get())
        # while True:
        #     print(r.status)
        r=app.AsyncResult(id='aa11c040-6683-4146-a9e7-b484eee8019f')
        print(r.get())

