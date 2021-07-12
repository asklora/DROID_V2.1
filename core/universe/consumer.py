import json
from channels.generic.websocket import WebsocketConsumer,AsyncWebsocketConsumer
from requests.api import patch
from datasource.rkd import RkdStream
import asyncio
import multiprocessing
import os
import signal

class UniverseConsumer(WebsocketConsumer):
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
        for t in multiprocessing.active_children():
            
            if t.name == self.room_group_name:
                if self.room_group_name in self.streaming_counter:
                    if self.channel_name in self.streaming_counter[self.room_group_name]['channel']:  self.streaming_counter[self.room_group_name]['channel'].remove(self.channel_name)
                    self.streaming_counter[self.room_group_name]['connection'] = len(self.streaming_counter[self.room_group_name]['channel'])
                    if self.streaming_counter[self.room_group_name]['connection'] < 1:
                        print(t.name,'terminated')
                        t.terminate()
                        if t.is_alive():
                            print(t.pid,'<<<pid')
                            os.kill(t.pid, signal.SIGKILL)
                        
        process = [{proc.name:proc}  for proc in multiprocessing.active_children()]
        print('process >>>',process)
        print('disconnect >>> ',self.streaming_counter)
        
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

    
    # Receive message from room group
    def broadcastmessage(self, event):
        # Send message to WebSocket
        self.send(text_data=json.dumps({
            'message': event['message']
        }))
    
    def streaming(self,event):
        proc_list = [p.name for p in multiprocessing.active_children()]
        if self.room_group_name in proc_list:
            pass
        else:
            rkd =  RkdStream.trkd_stream_initiate(event['message'])
            rkd.chanels = self.room_group_name
            proc = rkd.thread_stream()
            proc.daemon=True
            proc.start()
        
        if self.room_group_name in self.streaming_counter:
            pass
        else:
            self.streaming_counter[self.room_group_name] ={'connection':0,'channel':[]}
        if not self.channel_name  in self.streaming_counter[self.room_group_name]['channel']:
            self.streaming_counter[self.room_group_name]['channel'].append(self.channel_name)
            asyncio.run(self.channel_layer.send(
            self.channel_name,
            {
                'type':'broadcastmessage',
                'message': f'streaming {event["message"]} from firebase'
            }
            ))
        self.streaming_counter[self.room_group_name]['connection']=len(self.streaming_counter[self.room_group_name]['channel'])
        print("connected >>>> ",self.streaming_counter)
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