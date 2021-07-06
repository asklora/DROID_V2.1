from django.urls import re_path
from core.universe.consumer import UniverseConsumer


websocket_urlpatterns = [
    re_path(r"^ws/(?P<subscribe>[\w.]+)/$", UniverseConsumer.as_asgi()),
]
