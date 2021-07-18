import json
from channels.generic.websocket import WebsocketConsumer,AsyncWebsocketConsumer
from datasource.rkd import RkdStream
import asyncio
import multiprocessing
import os
import signal
from channels_presence.models import Room,Presence
from channels_presence.decorators import touch_presence

class UniverseConsumer(WebsocketConsumer):
    room_id =None


    def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['subscribe']
        self.room_group_name = self.room_name
        # Join room group
        asyncio.run(self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        ))
        self.accept()
        asyncio.run(self.channel_layer.send(
                self.channel_name,
                {
                    'type':'broadcastmessage',
                    'message': f'connection accepted to {self.room_group_name} channel',
                    'status':200
                }
                ))
        Room.objects.add(self.room_group_name,self.channel_name)
        self.room_id = Room.objects.get(channel_name=self.room_group_name)

    def disconnect(self, close_code):
        # Leave room group
        asyncio.run(self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        ))
        Room.objects.remove(self.room_group_name,self.channel_name)

        for t in multiprocessing.active_children():
            if t.name == self.room_group_name:
                try:
                    connection_list = Presence.objects.filter(room=self.room_id)
                    if not connection_list.exists():
                        print(t.name,'terminated')
                        t.terminate()
                        if t.is_alive():
                            print(t.pid,'<<<pid')
                            os.kill(t.pid, signal.SIGKILL)
                except Room.DoesNotExist:
                    pass
                        
        process = [{proc.name:proc}  for proc in multiprocessing.active_children()]
        print('process >>>',process)
        print("disconnected >>>> ",[con['channel_name'] for con in Presence.objects.filter(room=self.room_id).values('channel_name')])
    
    def force_close_connection(self,event):
        self.close()

    # Receive message from WebSocket
    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        # print(text_data_json)
        message = text_data_json['message']
        asyncio.run(self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': text_data_json['type'],
                'message': message
            }
        ))

    
    @touch_presence
    def ping(self,event):
        print("ping event",event)
        

    


    # Receive message from room group
    def broadcastmessage(self, event):
        # Send message to WebSocket
        event.pop('type')
        self.send(text_data=json.dumps(event))
    
    def streaming(self,event):
        if event['message']:
            proc_list = [p.name for p in multiprocessing.active_children()]
            if self.room_group_name in proc_list:
                pass
            else:
                rkd =  RkdStream.trkd_stream_initiate(event['message'])
                rkd.chanels = self.room_group_name
                proc = rkd.thread_stream()
                proc.daemon=True
                proc.start()
            asyncio.run(self.channel_layer.send(
            self.channel_name,
            {
                'type':'broadcastmessage',
                'message': f'streaming {event["message"]} from firebase',
                'status':200
            }
            ))
        else:
            asyncio.run(self.channel_layer.send(
                self.channel_name,
                {
                    'type':'broadcastmessage',
                    'message': f'message streaming cannot be empty, your payload was {event["message"]}',
                    'status':400
                }
                ))
            self.disconnect(400)
        print("connected >>>> ",[con['channel_name'] for con in Presence.objects.filter(room=self.room_id).values('channel_name')])
        print("payload >>>> ",event['message'])


class DurableConsumer(AsyncWebsocketConsumer):
    
    
    
    async def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['room_name']
        self.room_group_name =self.room_name

        # Join room group
        await self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        # if not self.run_task.done():
        #     # Clean up the task for the queue we created
        #     self.run_task.cancel()
        #     # try:
        #     #     # let us get any exceptions from the nested loop
        #     #     await self.run_task
        #     # except Exception:
        #     #     # Ignore this error as we just triggered it
        #     #     pass
        # else:
        #     # throw any error from this nested loop
        #     self.run_task.result()
        # Leave room group
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        )

    # Receive message from WebSocket
    async def receive(self, text_data):
        text_data_json = json.loads(text_data)
        message = text_data_json['message']
        type = text_data_json['type']

        # Send message to room group
        await self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': type,
                'message': message
            }
        )

    # Receive message from room group
    async def chat_message(self, event):
        message = event['message']

        # Send message to WebSocket
        await self.send(text_data=json.dumps({
            'message': message
        }))