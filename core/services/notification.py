from config.celery import app
from django.conf import settings




def send_notification(username:str,title:str,body:str):
    data={
                "type":"function",
                "module":"core.djangomodule.crudlib.notification.send_notif",
                "payload":{
                    "username":f"{username}",
                    "title":f"{title}",
                    "body":f"{body}",
                }
            }
    app.send_task("config.celery.listener",args=(data,),queue=settings.ASKLORA_QUEUE)


def send_bulk_notification(title:str,body:str):
    # TODO: need to check bulk message
    data={
                "type":"function",
                "module":"core.djangomodule.crudlib.notification.send_notif",
                "payload":{
                    "title":f"{title}",
                    "body":f"{body}",
                }
            }
    app.send_task("config.celery.listener",args=(data,),queue=settings.ASKLORA_QUEUE)