from websocket import create_connection
import json
ws = create_connection('ws://127.0.0.1:8000/ws/msft/')
ws.send(json.dumps({'message':['MSFT.O'],'type':'streaming'}))
while True:
    payload = json.loads(ws.recv())
    print(payload['message'])
    if payload['message'] == 'PING':
        print('reply')
        ws.send(json.dumps({
            'type':'ping',
            'message':'PONG'
        }))