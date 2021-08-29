"""
ASGI config for config project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/howto/deployment/asgi/
"""

import os
from django.core.asgi import get_asgi_application
import django
debug = os.environ.get('DJANGO_SETTINGS_MODULE',True)
if debug:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.production')
else:
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.development')
    
django.setup()
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from config.routing import websocket_urlpatterns
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack
from channels.sessions import SessionMiddlewareStack
from core.Clients.TWSClient import TwsApi
import signal
application = ProtocolTypeRouter({
    # Django's ASGI application to handle traditional HTTP requests
    "http": ASGIStaticFilesHandler(get_asgi_application()),
    # WebSocket chat handler
    "websocket": SessionMiddlewareStack(
        URLRouter(websocket_urlpatterns)
    ),
})
print('sgi')
if os.environ.get("RUN_MAIN",None):
    tws = TwsApi(256)
    tws.run()
    signal.signal(signal.SIGINT, tws.stop)
    print(tws)