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
import json
# data = json.dumps({"event_type": "build_image"})
# 13294075
# response =requests.post('https://api.github.com/repos/asklora/DROID_V2.1',data=data,headers={
#     "Authorization": "token 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",
#     "Accept": "application/vnd.github.v3+json"
# })
workflow = requests.get('https://api.github.com/repos/asklora/DROID_V2.1/actions/workflows',headers={
    "Authorization": "token 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",
    "Accept": "application/vnd.github.v3+json"
})
print(workflow.json())
print(workflow.status_code)