from config.celery import app
from django.apps import apps
from core.djangomodule.calendar import TradingHours
from core.djangomodule.general import UnixEpochDateField
from django.db import transaction
from datetime import datetime
from rest_framework import serializers
from channels.layers import get_channel_layer
import time
import json
import asyncio

class OrderDetailsServicesSerializers(serializers.ModelSerializer):
    created = UnixEpochDateField()
    filled_at = UnixEpochDateField()
    placed_at = UnixEpochDateField()
    canceled_at = UnixEpochDateField()

    class Meta:
        model = apps.get_model('orders', 'Order')
        fields = ['ticker','price','bot_id','amount','side',
        'order_uid','status','setup','created','filled_at',
        'placed','placed_at','canceled_at','qty']

@app.task(bind=True)
def order_executor(self,payload):
    payload = json.loads(payload)
    Model = apps.get_model('orders', 'Order')
    order = Model.objects.get(order_uid=payload['order_uid'])
    with transaction.atomic():
        if payload['status'] == 'cancel':
            order.status = payload['status']
            order.canceled_at = datetime.now()
            order.save()
        elif payload['status'] == 'placed':
            order.status = payload['status']
            order.placed = True
            order.placed_at = datetime.now()
            order.save()
    
    time.sleep(10)
    print('placed')
    
    market = TradingHours(mic=order.ticker.mic)
    if market.is_open:
        order.status = 'filled'
        order.filled_at = datetime.now()
        order.save()
        print('open')
        messages = 'order_accepted'
    else:
        messages = 'order_pending'


    
    payload = OrderDetailsServicesSerializers(order).data
    channel_layer = get_channel_layer()
    asyncio.run(channel_layer.group_send(self.request.id,
        {
            'type':'send_order_message',
            'message_type':messages,
            'message':'order update',
            'payload':payload
        }))
    return payload
