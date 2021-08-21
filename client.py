from websocket import create_connection
import json
ws = create_connection('ws://127.0.0.1:8000/orders/')
ws.send(json.dumps({'request_id':'room'}))
while True:
    payload = json.loads(ws.recv())
    print(payload)
    if payload['message'] == 'PING':
        print('reply')
        ws.send(json.dumps({
            'type':'ping',
            'message':'PONG',
            'user':'agam'
        }))