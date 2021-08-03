import json
from channels.generic.websocket import WebsocketConsumer, AsyncWebsocketConsumer
from datasource.rkd import RkdStream
import asyncio
import multiprocessing
import os
import signal
from channels_presence.models import Room, Presence
from channels_presence.decorators import touch_presence


class UniverseConsumer(WebsocketConsumer):
    room_id = None

    def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['subscribe']
        self.room_group_name = self.room_name
        if self.room_name != 'market':
            self.close()
        # Join room group
        asyncio.run(self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        ))
        self.accept()
        asyncio.run(self.channel_layer.send(
            self.channel_name,
            {
                    'type': 'broadcastmessage',
                    'message': f'connection accepted to {self.room_group_name} channel',
                    'status': 200
                    }
        ))
        Room.objects.add(self.room_group_name, self.channel_name)
        self.room_id = Room.objects.get(channel_name=self.room_group_name)

    def disconnect(self, close_code):
        # Leave room group
        asyncio.run(self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        ))
        Room.objects.remove(self.room_group_name, self.channel_name)

        for t in multiprocessing.active_children():
            if t.name == self.room_group_name:
                try:
                    connection_list = Presence.objects.filter(
                        room=self.room_id)
                    if not connection_list.exists():
                        print(t.name, 'terminated')
                        t.terminate()
                        if t.is_alive():
                            print(t.pid, '<<<pid')
                            os.kill(t.pid, signal.SIGKILL)
                except Room.DoesNotExist:
                    pass

        process = [{proc.name: proc}
                   for proc in multiprocessing.active_children()]
        print('process >>>', process)
        print("disconnected >>>> ", [con['channel_name'] for con in Presence.objects.filter(
            room=self.room_id).values('channel_name')])

    def force_close_connection(self, event):
        self.close()

    # Receive message from WebSocket
    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        # print(text_data_json)
<<<<<<< HEAD
=======
        # message = text_data_json['message']
>>>>>>> e85d12cc31ff02a29bc97d2008dd775402d7a0b2
        asyncio.run(self.channel_layer.group_send(
            self.room_group_name,
            text_data_json
        ))

    @touch_presence
    def ping(self, event):
        print("ping event", event)
        process = [{proc.name: proc}
                   for proc in multiprocessing.active_children()]
        if not process:
            rkd = RkdStream()
            rkd.chanels = self.room_group_name
            proc = rkd.thread_stream()
            proc.daemon = True
            proc.start()

    # Receive message from room group

    def broadcastmessage(self, event):
        # Send message to WebSocket
        event.pop('type')
        self.send(text_data=json.dumps(event))

    def streaming(self, event):
        if event['message']:
            proc_list = [p.name for p in multiprocessing.active_children()]
            if self.room_group_name in proc_list:
                pass
            else:

                rkd = RkdStream()
                rkd.chanels = self.room_group_name
                proc = rkd.thread_stream()
                proc.daemon = True
                proc.start()
            asyncio.run(self.channel_layer.send(
                self.channel_name,
                {
                    'type': 'broadcastmessage',
                    'message': f'streaming  from firebase',
                    'status': 200
                }
            ))
        else:
            asyncio.run(self.channel_layer.send(
                self.channel_name,
                {
                    'type': 'broadcastmessage',
                    'message': f'message streaming cannot be empty, your payload was {event["message"]}',
                    'status': 400
                }
            ))
            self.disconnect(400)
        print("connected >>>> ", [con['channel_name'] for con in Presence.objects.filter(
            room=self.room_id).values('channel_name')])
        print("payload >>>> ", event['message'])


class OrderConsumer(AsyncWebsocketConsumer):

    payload = {
        'message_type': 'connect',
        'message': '',
        'status_code': 200,
        'payload': None
    }

    async def connect(self):
        # self.room_group_name =self.scope['url_route']['kwargs']['uid']

        # Join room group
        # await self.channel_layer.group_add(
        #     self.room_group_name,
        #     self.channel_name
        # )

        await self.accept()
        await self.channel_layer.group_add(
            'anonym_channel',
            self.channel_name
        )

    async def disconnect(self, close_code):
        if hasattr(self, 'room_group_name'):
            await self.channel_layer.group_discard(
                self.room_group_name,
                self.channel_name
            )
        else:
            await self.channel_layer.group_discard(
                'anonym_channel',
                self.channel_name
            )

    async def force_close(self):
        await asyncio.sleep(10)
        await self.close()

    # Receive message from WebSocket
    async def receive(self, text_data):
        text_data_json = json.loads(text_data)
        # Send message to room group
        # message = text_data_json['message']
        if 'action_id' in text_data_json:
            self.room_group_name = text_data_json['action_id']
            await self.channel_layer.group_add(
                text_data_json['action_id'],
                self.channel_name
            )
            self.payload['message'] = f'subscribed to {text_data_json["action_id"]}'
            self.payload['type'] = 'send_message'
            await self.channel_layer.group_send(
                text_data_json['action_id'],
                self.payload
            )
        else:
            self.payload['message'] = f'payload doesnt have action_id connection will terminate in 10 second'
            self.payload['type'] = 'send_message'
            self.payload['status_code'] = 403
            self.payload['message_type'] = 'rejected'
            self.room_group_name = 'rejected_channels'
            await self.channel_layer.send(
                self.channel_name,
                self.payload
            )
            asyncio.ensure_future(self.force_close())

    # Receive message from room group

    async def send_message(self, event):
        event.pop('type')

        # Send message to WebSocket
        await self.send(text_data=json.dumps(event))

    async def send_order_message(self, event):
        event.pop('type')

        # Send message to WebSocket
        await self.send(text_data=json.dumps(event))
        asyncio.ensure_future(self.force_close())
