from django.urls import re_path
from core.universe.consumer import UniverseConsumer,DurableConsumer


websocket_urlpatterns = [
    re_path(r"^ws/(?P<subscribe>[\w.]+)/$", UniverseConsumer.as_asgi()),
    re_path(r"^new/(?P<room_name>[\w.]+)/$", DurableConsumer.as_asgi()),
]
