from datetime import datetime
import boto3
import time
import sys
import getopt
import socket
import json
import requests
import websocket
from datasource.rkd import Rkd
from core.universe.models import Universe
from django.core.management.base import BaseCommand, CommandError
from datasource.rkd import RkdData
"""
Global Variables 
position : remote address / your IP

"""
# Start websocket handshake


class Command(BaseCommand):
    def __init__(self):
        self.rkd = RkdData()
        self.user = self.rkd.credentials.username
        self.app_id = self.rkd.credentials.extra_data['key']
        self.password = self.rkd.credentials.password
        self.position = socket.gethostbyname(socket.gethostname())
        self.web_socket_app = None
        self.token = self.rkd.credentials.token
        super().__init__()

    def handle(self, *args, **options):

        ws_address = "wss://streaming.trkd.thomsonreuters.com/WebSocket"
        print("Connecting to WebSocket " + ws_address + " ...")
        web_socket_app = websocket.WebSocketApp(ws_address, header=['User-Agent: Python'],
                            on_message=self.on_message,
                            on_error=self.on_error,
                            on_close=self.on_close,
                            subprotocols=['tr_json2'])
        web_socket_app.on_open = self.on_open
        try:
            web_socket_app.run_forever()
        except Exception as e:
            print(f"==========={e}=============")
            web_socket_app.close()
     
    

    def answer_ping(self, ws, *args, **options): 
        ping_req = {"Type":"Pong"}
        ws.send(json.dumps(ping_req))
        print("SENT:")
        print(json.dumps(ping_req, sort_keys=True, indent=2, separators=(',', ':')))

    def write_on_s3(self, message, *args, **options):
        try:
            ticker = message['Key']['Name']
            type_name = message['Type']
            if type_name == 'Update':
                act = message['UpdateType']
            else:
                act = ""
            note = json.dumps(message)
            s3 = boto3.client('s3', aws_access_key_id='AKIA2XEOTUNGWEQ43TB6' ,aws_secret_access_key='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', region_name='ap-east-1')
            # epoch = str(int(datetime.now().timestamp()))
            # s3_file = 'test_dir/'+str(int(epoch)) + ".txt"
            s3_file = "test_dir/"+type_name+"-"+ticker+"-"+act+".txt"
            upload = s3.put_object(Body=note, Bucket='droid-v2-logs', Key=s3_file)
        except Exception as e:
            print(f"Error ===== {e}")

    def process_message(self, ws, message_json, *args, **options):
        """ Parse at high level and output JSON of message """
        message_type = message_json['Type']
       
        """ check for login response """
        if message_type == "Refresh":
            if 'Domain' in message_json:
                message_domain = message_json['Domain']
                if message_domain == "Login":
                    self.process_login_response(ws, message_json)
                    return
            else:
                self.write_on_s3(message_json)
        elif message_type == "Ping":
            self.answer_ping(ws)
        elif message_type == "Update":
            if message_json['UpdateType'] == 'Quote':
                print(f"====== Quote - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json)
            elif message_json['UpdateType'] == 'Trade':
                print(f"====== Trade - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json)
            elif message_json['UpdateType'] == 'Unspecified':
                print(f"====== Unspecified - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json)
            else:
                None
            # write_on_s3(message_json)
     
        """ Else it's market price response, so now exit this simple example """
        # web_socket_app.close()
     
    def process_login_response(self, ws, message_json, *args, **options):
        print ("Logged in!")
        self.send_market_price_request(ws)

    def beautify_print(self, message, *args, **options):
        change = {
                'CF_ASK': 'intraday_ask', 
                'CF_CLOSE': 'close', 
                'CF_BID': 'intraday_bid', 
                'CF_HIGH': 'high', 'CF_LOW': 'low',
                'PCTCHNG': 'latest_price_change', 
                'TRADE_DATE': 'last_date',
                'CF_VOLUME':'volume',
                'CF_LAST':'latest_price'
            }
            
        for key, val in message['Fields'].items():
            # print(f"{key} : {val}")
            data = [
                    {
                        "ticker":message['Key']['Name'],
                        change[key]:val
                    }
                ]
            print(data)
            try:
                self.rkd.save('master', 'LatestPrice', data)
            except Exception as e:
                print(e)
       
    def send_market_price_request(self, ws, *args, **options):
        """ Create and send simple Market Price request """

        HKD_universe = [ ticker['ticker'] for ticker in Universe.objects.filter(currency_code='HKD',is_active=True).exclude(ticker__in=['.HSI','.HSLI']).values('ticker')]

        mp_req_json = {
        'ID': int(time.time()), 
        'Key': {
            'Name': HKD_universe
            },
        'View':[
            'PCTCHNG',
            'CF_CLOSE',
            'CF_ASK',
            'CF_BID',
            'CF_HIGH',
            'CF_LOW',
            'CF_LAST',
            'CF_VOLUME',
            'TRADE_DATE'
            ]
        }

        ws.send(json.dumps(mp_req_json))
        print("SENT:")
        print(json.dumps(mp_req_json, sort_keys=True, indent=2, separators=(',', ':')))
     
    def send_login_request(self, ws, *args, **options):
        """ Generate a login request from command line data (or defaults) and send """
        login_json = { 'ID': 1, 'Domain': 'Login',
            'Key': { 'NameType': 'AuthnToken', 'Name': '',
                'Elements': { 'AuthenticationToken':'', 'ApplicationId': '', 'Position': '' }
            }
        }
        login_json['Key']['Name'] = self.user
        login_json['Key']['Elements']['AuthenticationToken'] = self.token
        login_json['Key']['Elements']['ApplicationId'] = self.app_id
        login_json['Key']['Elements']['Position'] = self.position
        ws.send(json.dumps(login_json))
        print("SENT LOGIN REQUEST:")
        print(json.dumps(login_json, sort_keys=True, indent=2, separators=(',', ':')))
     
    def on_message(self, ws, message, *args, **options):
        """ Called when message received, parse message into JSON for processing """
        print("")
        print("======== RECEIVED: ========")
        # print(f"ws == {ws}, msg == {message}, args == {args}, option == {options}")
        message_json = json.loads(message)
        # print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))

     
        for singleMsg in message_json:
            self.process_message(ws, singleMsg)

    def on_error(self, ws, error, *args, **options):
        """ Called when websocket error has occurred """
        print(error)
     
    def on_close(self, ws, *args, **options):
        # print(super(on_close, self))
        """ Called when websocket is closed """
        print("WebSocket Closed")
     
    def on_open(self, ws, *args, **options):
        """ Called when handshake is complete and websocket is open, send login """
        print("WebSocket open!")
        self.send_login_request(ws)

    

