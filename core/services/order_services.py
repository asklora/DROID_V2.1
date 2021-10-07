from bot.calculate_bot import populate_daily_profit
from config.celery import app
from django.apps import apps
from core.djangomodule.general import logging
from core.user.models import User
from core.services.models import ErrorLog
from django.db import transaction
from firebase_admin import messaging
from datetime import datetime
from rest_framework import serializers
from channels.layers import get_channel_layer
from ingestion import firebase_user_update
from datasource import rkd as trkd
import time
import json
import asyncio

class OrderDetailsServicesSerializers(serializers.ModelSerializer):

    class Meta:
        model = apps.get_model('orders', 'Order')
        fields = ['ticker', 'price', 'bot_id', 'amount', 'side',
                  'order_uid', 'status', 'setup', 'created', 'filled_at',
                  'placed', 'placed_at', 'canceled_at', 'qty','user_id']

@app.task(bind=True)
def pending_order_checker(self):
    Exchange = apps.get_model('universe', 'ExchangeMarket')
    Order = apps.get_model('orders', 'Order')
    orders = Order.objects.prefetch_related('ticker').filter(status='pending')
    orders_id=[]
    if orders.exists():
        for order in orders:
            market_db = Exchange.objects.get(mic=order.ticker.mic)
            if market_db.is_open:
                fb_token=None
                if 'firebase_token' in order.order_summary:
                    fb_token = order.order_summary['firebase_token']
                payload = {'order_uid': str(order.order_uid),'status':'placed'}
                if fb_token:
                    payload['firebase_token'] = fb_token
                
                payload = json.dumps(payload)
                order_executor.apply_async(args=(payload,),kwargs={"recall":True},task_id=str(order.order_uid))
                orders_id.append(str(order.order_uid))

    return {'success':'order pending executed','data':orders_id}






@app.task(bind=True)
def order_executor(self, payload, recall=False):
    """
    #TODO: ERROR HANDLING HERE AND RETURN MESSAGE TO USER AND SOCKET
    
    """
    payload = json.loads(payload)
    Model = apps.get_model('orders', 'Order')
    Exchange = apps.get_model('universe', 'ExchangeMarket')
    try:
        order = Model.objects.get(order_uid=payload['order_uid'])
    except Model.DoesNotExist:
        return {'err':f"{payload['order_uid']} doesnt exists"}

    if recall:
        if order.status != 'pending':
            return {'err':'order already executed','data':{'order_id':str(order.order_uid),'status':order.status}}
        rkd = trkd.RkdData()
        # getting new price
        df = rkd.get_quote([order.ticker.ticker], df=True)
        df['latest_price'] = df['latest_price'].astype(float)
        ticker = df.loc[df["ticker"] == order.ticker.ticker]
        order.price = ticker.iloc[0]['latest_price']
        TransactionHistory = apps.get_model('user', 'TransactionHistory')
        in_wallet_transactions = TransactionHistory.objects.filter(
            transaction_detail__order_uid=str(order.order_uid))
        if in_wallet_transactions.exists():
            in_wallet_transactions.delete()
        
        # for apps, need to change later with better logic
        if order.side == 'buy' and order.is_app_order and order.is_init:
            if (order.amount / order.margin) > 10000:
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

    # time.sleep(10)
    if not order.canceled_at:
        """
        should not go here if canceled
        """
        if not order.is_bot_order or order.side=='sell':
            share = order.qty
        else:
            share = order.setup['performance']['share_num']
            
        market_db = Exchange.objects.get(mic=order.ticker.mic)
        if market_db.is_open:
            if not order.insufficient_balance():
                order.status = 'filled'
                order.filled_at = datetime.now()
                order.save()

                print('open')
                messages = 'order accepted'
                message = f'{order.side} order {share} stocks {order.ticker.ticker} was executed, status filled'
            else:
                order.status = 'cancel'
                order.canceled_at = datetime.now()
                order.save()
                messages = 'order rejected'
                message = f'{order.side} order {share} stocks {order.ticker.ticker} is rejected due insufficient funds, status canceled'
        else:
            print('close')
            messages = 'order pending'
            message = f'{order.side} order {share} stocks {order.ticker.ticker} is received, status pending'
            if payload.get('firebase_token',None):
                filttered_order = Model.objects.filter(order_uid=order.order_uid)
                filttered_order.update(order_summary={'firebase_token':payload['firebase_token']})
    else:
        """
        we need message if order is cancel
        """
        messages = 'order canceled'
        message = f'{order.side} order  stocks {order.ticker.ticker} is canceled'
    
    order.populate_to_firebase()
    payload_serializer = OrderDetailsServicesSerializers(order).data
    channel_layer = get_channel_layer()
    if payload.get('firebase_token',None):
        msg = messaging.Message(
            notification=messaging.Notification(
                title=messages,
                body=message
            ),
            token=payload['firebase_token'],
        )
        try:
            res = messaging.send(msg)
            logging.info(res)
        except Exception as e:
            logging.error(str(e))

                
    asyncio.run(channel_layer.group_send(self.request.id,
                                         {
                                             'type': 'send_order_message',
                                             'message_type': messages,
                                             'message': 'order update',
                                             'payload': payload_serializer,
                                             'status_code': 200
                                         }))
    return payload_serializer



@app.task(bind=True)
def cancel_pending_order(self,from_date:datetime=datetime.now(),run_async=False):
    Order = apps.get_model('orders', 'Order')
    orders = Order.objects.prefetch_related('ticker').filter(status='pending')
    if orders.exists():
        for order in orders:
            payload = json.dumps({'order_uid': str(order.order_uid),'status':'cancel'})
            if run_async:
                order_executor.apply_async(args=(payload,),task_id=str(order.order_uid))
            else:
                order_executor(payload)
    return {'success':'order pending canceled'}

@app.task
def update_rtdb_user_porfolio():
    
    try:
        # hkd_exchange =ExchangeMarket.objects.get(mic='XHKG')
        # if hkd_exchange.is_open:
        users = [user['id'] for user in User.objects.filter(is_superuser=False,current_status="verified").values('id')]
        populate_daily_profit()
        firebase_user_update(user_id=users)
    except Exception as e:
        err = ErrorLog.objects.create_log(
        error_description=f"===  ERROR IN UPDATE REALTIME PORTFOLIO FIREBASE ===", error_message=str(e))
        err.send_report_error()
        return {'error':str(e)}
    return {'status':'updated firebase portfolio'}









    