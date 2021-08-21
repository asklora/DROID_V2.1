from django.urls import re_path
from core.universe.consumer import UniverseConsumer,OrderConsumer,TestConsumer


websocket_urlpatterns = [
    re_path(r"^ws/(?P<subscribe>[\w.]+)/$", UniverseConsumer.as_asgi()),
    re_path(r"^orders/$", OrderConsumer.as_asgi()),
    re_path(r"^test/$", TestConsumer.as_asgi()),
]
