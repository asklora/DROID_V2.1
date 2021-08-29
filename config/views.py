from django.http import HttpResponse
from config.asgi import tws


def IndexWs(request):
    tws.get_contract('MSFT')
    return HttpResponse('ok')