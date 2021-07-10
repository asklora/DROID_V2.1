import json
from asgiref.sync import async_to_sync,sync_to_async
from channels.generic.websocket import WebsocketConsumer,AsyncWebsocketConsumer
from datasource.rkd import RkdStream
import asyncio
import multiprocessing
from channels.layers import get_channel_layer

class UniverseConsumer(WebsocketConsumer):
    thread = []
    streaming_counter ={}


    def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['subscribe']
        self.room_group_name = self.room_name
        # Join room group
        asyncio.run(self.channel_layer.group_add(
            self.room_group_name,
            self.channel_name
        ))
        self.accept()


    def disconnect(self, close_code):
        # Leave room group
        asyncio.run(self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name
        ))
        for t in self.thread:
            if t.name == self.room_group_name:
                print(t.name)
                t.terminate()

        
    # Receive message from WebSocket
    def receive(self, text_data):
        text_data_json = json.loads(text_data)
        print(text_data_json)
        message = text_data_json['message']
        asyncio.run(self.channel_layer.group_send(
            self.room_group_name,
            {
                'type': text_data_json['type'],
                'message': message
            }
        ))

    # Receive message from room group
    def broadcastmessage(self, event):
        # Send message to WebSocket
        self.send(text_data=json.dumps({
            'message': event
        }))
    
    def streaming(self,event):
        rkd =  RkdStream()
        rkd.ticker_data = event['message']
        rkd.chanels = self.room_group_name
        if multiprocessing.active_children():
            for p in multiprocessing.active_children():
                if p.name == self.room_group_name:
                    self.send(text_data=json.dumps({
                    'message': f'stream {event["message"]}'
                    }))
                # else:
                #     proc = rkd.thread_stream()
                #     proc.daemon=True
                #     proc.start()
                #     self.thread.append(proc)
        else:
            proc = rkd.thread_stream()
            # thread = threading.Thread(target=rkd.stream)
            # threading_object = {'name':self.room_group_name,'threads':thread}
            proc.daemon=True
            proc.start()
            self.thread.append(proc)
            
            self.send(text_data=json.dumps({
                    'message': f'stream {event["message"]}'
                    }))
        if self.room_group_name in self.streaming_counter:
                self.streaming_counter[self.room_group_name]+=1
        else:
            self.streaming_counter[self.room_group_name] = 1
        print(self.streaming_counter)



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
    
    
    # @sync_to_async
    # def rkd_init(self):
    #     return RkdStream()
    
    
    # @sync_to_async
    # def rkd_stream(self,rkd):
    #     return rkd.stream()


    # async def streaming(self,event):
    #     rkd =  await self.rkd_init()
    #     rkd.ticker_data = event['message']
    #     rkd.chanels = self.room_group_name
    #     self.run_task = asyncio.create_task(self.rkd_stream(rkd))
    #      # Send message to room group
    #     await self.channel_layer.group_send(
    #         self.room_group_name,
    #         {
    #             'type': 'chat_message',
    #             'message': 'stopped'
    #         }
    #     )