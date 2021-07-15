from websocket import create_connection
import json
ws = create_connection('ws://16.162.110.123/ws/msft/')
ws.send(json.dumps({'message':['MSFT.O'],'type':'streaming'}))
while True:
    print(ws.recv())