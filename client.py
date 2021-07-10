from websocket import create_connection
import json
ws = create_connection('ws://127.0.0.1:8000/ws/aapl/')
ws.send(json.dumps({'message':['AAPL.O'],'type':'streaming'}))
while True:
    print(ws.recv())