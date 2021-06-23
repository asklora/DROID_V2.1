#!/usr/bin/env python
""" Simple example of outputting Market Price JSON data using Websockets """
import sys
import getopt
import socket
import json
import requests
import websocket
 
# Global Variables
user = ''
app_id = ''
position = socket.gethostbyname("")
password = ''
token=''
web_socket_app = None
 
def send_market_price_request(ws):
    """ Create and send simple Market Price request """
    mp_req_json = {"ID": 2,
    "Domain": "MarketPrice",
    "Key": {
        "Name": [
            "005930.KS",
        ]
    },
    "View": [
        "PCTCHNG",
        "CF_CLOSE"
    ]}
    ws.send(json.dumps(mp_req_json))
    print("SENT:")
    print(json.dumps(mp_req_json, sort_keys=True, indent=2, separators=(',', ':')))



def process_login_response(ws, message_json):
    print ("Logged in!")
    send_market_price_request(ws)

def process_message(ws, message_json):
    """ Parse at high level and output JSON of message """
    message_type = message_json['Type']
 
    """ check for login response """
    if message_type == "Refresh":
        if 'Domain' in message_json:
            message_domain = message_json['Domain']
            if message_domain == "Login":
                process_login_response(ws, message_json)
                return
    
        """ Else it's market price response, so now exit this simple example """
        # web_socket_app.close()
 

 

 
def send_login_request(ws):
    """ Generate a login request from command line data (or defaults) and send """
    login_json = { 'ID': 1, 'Domain': 'Login',
        'Key': { 'NameType': 'AuthnToken', 'Name': '',
            'Elements': { 'AuthenticationToken':'', 'ApplicationId': '', 'Position': '' }
        }
    }
    login_json['Key']['Name'] = user
    login_json['Key']['Elements']['AuthenticationToken'] = token
    login_json['Key']['Elements']['ApplicationId'] = app_id
    login_json['Key']['Elements']['Position'] = position
    ws.send(json.dumps(login_json))
    print("SENT:")
    print(json.dumps(login_json, sort_keys=True, indent=2, separators=(',', ':')))
 
def on_message(ws, message):
    """ Called when message received, parse message into JSON for processing """
    print("RECEIVED: ")
    message_json = json.loads(message)
    print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))
 
    for singleMsg in message_json:
        process_message(ws, singleMsg)
 
def on_error(ws, error):
    """ Called when websocket error has occurred """
    print(error)
 
def on_close(ws):
    """ Called when websocket is closed """
    print("WebSocket Closed")
 
def on_open(ws):
    """ Called when handshake is complete and websocket is open, send login """
    print("WebSocket open!")
    send_login_request(ws)
 
if __name__ == "__main__":
 
    # Get command line parameters
    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["app_id=", "user=", "password="])
    except getopt.GetoptError:
        print('Usage: trkd_websocketexample.py [--app_id app_id] [--user user] [--password password] ')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("--app_id"):
            app_id = arg
        elif opt in ("--user"):
            user = arg
        elif opt in ("--password"):
            password = arg
 
    # get trkd token
    tm_url ='https://api.rkd.refinitiv.com/api/TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1'
    tm_req = '{{ "CreateServiceToken_Request_1": {{ "ApplicationID": "{0}", "Username": "{1}", "Password": "{2}" }} }} '
    tm_req = tm_req.format(app_id, user, password)
    tm_res = requests.post(tm_url, data=tm_req, headers = {'Content-type': 'application/json'})
    token = tm_res.json()['CreateServiceToken_Response_1']['Token']
 
    ws_address = "wss://streaming.trkd.thomsonreuters.com/WebSocket"
    print("Connecting to WebSocket " + ws_address + " ...")
    web_socket_app = websocket.WebSocketApp(ws_address, header=['User-Agent: Python'],
                        on_message=on_message,
                        on_error=on_error,
                        on_close=on_close,
                        subprotocols=['tr_json2'])
    web_socket_app.on_open = on_open
 
    try:
        web_socket_app.run_forever()
    except:
        web_socket_app.close()