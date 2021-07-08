import json
from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer
from datasource import rkd
from .models import Universe


class UniverseConsumer(WebsocketConsumer):
    def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['subscribe']
        self.room_group_name = self.room_name
        # Join room group
        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )
        self.accept()


    def disconnect(self, close_code):
        # Leave room group
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name
        )

        
    # Receive message from WebSocket
    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json['message']
        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                'type': 'broadcastmessage',
                'message': message
            }
        )

    # Receive message from room group
    def broadcastmessage(self, event):
        print(event)
        # Send message to WebSocket
        self.send(text_data=json.dumps({
            'message': event
        }))