from config.celery import app
from django.apps import apps
from core.djangomodule.calendar import TradingHours
from core.djangomodule.general import UnixEpochDateField
from django.db import transaction
from firebase_admin import messaging
from datetime import datetime
from rest_framework import serializers
from channels.layers import get_channel_layer
from datasource.rkd import RkdData
import json
import asyncio


class OrderDetailsServicesSerializers(serializers.ModelSerializer):
    # created = UnixEpochDateField()
    # filled_at = UnixEpochDateField()
    # placed_at = UnixEpochDateField()
    # canceled_at = UnixEpochDateField()

    class Meta:
        model = apps.get_model('orders', 'Order')
        fields = ['ticker', 'price', 'bot_id', 'amount', 'side',
                  'order_uid', 'status', 'setup', 'created', 'filled_at',
                  'placed', 'placed_at', 'canceled_at', 'qty']


@app.task(bind=True)
def order_executor(self, payload, recall=False):
    payload = json.loads(payload)
    Model = apps.get_model('orders', 'Order')
    try:
        order = Model.objects.get(order_uid=payload['order_uid'])
    except Model.DoesNotExist:
        return {'err':f"{payload['order_uid']} doesnt exists"}

    if recall:
        rkd = RkdData()
        # getting new price
        df = rkd.get_quote([order.ticker.ticker], df=True)
        df['latest_price'] = df['latest_price'].astype(float)
        ticker = df.loc[df["ticker"] == order.ticker.ticker]
        order.price = ticker.iloc[0]['latest_price']
        TransactionHistory = apps.get_model('user', 'TransactionHistory')
        in_wallet_transactions = TransactionHistory.objects.filter(
            transaction_detail__order_uid=str(order.order_uid))
        if in_wallet_transactions.exists():
            in_wallet_transactions.get().delete()
        
        # FIXME: for apps, need to change later with better logic
        if order.order_type=='apps':
            if order.amount >= 10000:
                order.amount = 20000
            else:
                order.amount = 10000
        order.status = 'review'
        order.placed = False
        order.placed_at = None
        with transaction.atomic():
            order.save()

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

    # debug only
    # time.sleep(10)

    print('placed')
    if order.bot_id == 'STOCK_stock_0':
        share = order.qty
    else:
        share = order.setup['share_num']
    market = TradingHours(mic=order.ticker.mic)
    if market.is_open:
        order.status = 'filled'
        order.filled_at = datetime.now()
        order.save()

        print('open')
        messages = 'order accepted'
        message = f'{order.side} order {share} stocks {order.ticker.ticker} was executed, status filled'
    else:
        print('close')
        messages = 'order pending'
        message = f'{order.side} order {share} stocks {order.ticker.ticker} was executed, status pending'
        # create schedule to next bell and will recrusive until market status open
        # still keep sending message. need to improve
        order_executor.apply_async(args=(json.dumps(payload),), kwargs={
                                   'recall': True}, eta=market.next_bell)

    payload_serializer = OrderDetailsServicesSerializers(order).data
    channel_layer = get_channel_layer()
    if 'firebase_token' in payload:
        if payload['firebase_token']:
            msg = messaging.Message(
                notification=messaging.Notification(
                    title=messages,
                    body=message
                ),
                token=payload['firebase_token'],
            )
            res = messaging.send(msg)
            print(res)
    asyncio.run(channel_layer.group_send(self.request.id,
                                         {
                                             'type': 'send_order_message',
                                             'message_type': messages,
                                             'message': 'order update',
                                             'payload': payload_serializer,
                                             'status_code': 200
                                         }))
    return payload_serializer
