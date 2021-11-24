# from websocket import create_connection
# import json
# ws = create_connection('wss://services.asklora.ai/ws/market/')
# ws.send(json.dumps({'type':'streaming','message':'i need to stream'}))
# while True:
#     payload = json.loads(ws.recv())
#     print(payload)
#     if payload['message'] == 'PING':
#         print('reply')
#         ws.send(json.dumps({
#             'type':'ping',
#             'message':'PONG',
#             'user':'python-client'
#         }))
import requests

import math

a=math.ceil(1.54)
print(a)
def widget(symbols):
    url = f'https://www.tradingview.com/mediumwidgetembed/?symbols={symbols}'
    return url
