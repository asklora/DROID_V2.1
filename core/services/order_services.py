from bot.calculate_bot import populate_daily_profit
from config.celery import app
from django.apps import apps
from core.user.models import User
from core.services.models import ErrorLog
from datetime import datetime
from rest_framework import serializers
from ingestion.firestore_migration import firebase_user_update
import json
import asyncio
from general.slack import report_to_slack
from core.orders.factory.orderfactory import order_executor
class OrderDetailsServicesSerializers(serializers.ModelSerializer):

    class Meta:
        model = apps.get_model('orders', 'Order')
        fields = ['ticker', 'price', 'bot_id', 'amount', 'side',
                  'order_uid', 'status', 'setup', 'created', 'filled_at',
                  'placed', 'placed_at', 'canceled_at', 'qty','user_id']

@app.task(bind=True)
def pending_order_checker(self,currency=None):
    Exchange = apps.get_model('universe', 'ExchangeMarket')
    Order = apps.get_model('orders', 'Order')
    orders_to_check = Order.objects.prefetch_related('ticker').filter(status='pending',ticker__currency_code=currency)
    duplicate_order_id=[]
    for user in orders_to_check:
        user_order = orders_to_check.filter(user_id=user.user_id,status='pending',ticker=user.ticker,bot_id=user.bot_id)
        if user_order.count() > 1:
            for duplicate in user_order:
                if not str(duplicate.order_uid) in duplicate_order_id:
                    duplicate_order_id.append(str(duplicate.order_uid))
    
    if duplicate_order_id:
        report_to_slack(f'duplicate order found and skip to execute, \n {tuple(duplicate_order_id)}',"#error-log")
        
        
    orders = Order.objects.prefetch_related('ticker').filter(
        status='pending',ticker__currency_code=currency
        ).exclude(order_uid__in=duplicate_order_id)
    
    
    orders_id=[]
    if orders.exists():
        for order in orders:
            market_db = Exchange.objects.get(mic=order.ticker.mic)
            if market_db.is_open:
                fb_token=None
                if 'firebase_token' in order.order_summary:
                    fb_token = order.order_summary['firebase_token']
                payload = {'order_uid': str(order.order_uid),'status':'placed','side':order.side}
                payload['firebase_token'] = fb_token
                
                payload = json.dumps(payload)
                # order_executor.apply_async(args=(payload,),kwargs={"recall":True},task_id=str(order.order_uid))
                order_executor(payload)
                orders_id.append(str(order.order_uid))

    return {'success':'order pending executed','data':orders_id}






@app.task(bind=True)
def cancel_pending_order(self,from_date:datetime=datetime.now(),run_async=False):
    Order = apps.get_model('orders', 'Order')
    orders = Order.objects.prefetch_related('ticker').filter(status='pending')
    if orders.exists():
        for order in orders:
            payload = json.dumps({'order_uid': str(order.order_uid),'status':'cancel'})
            order_executor(payload)
    return {'success':'order pending canceled'}




@app.task(ignore_result=True)
def update_rtdb_user_porfolio():
    
    try:
        users = [user['id'] for user in User.objects.filter(is_superuser=False,current_status="verified").values('id')]
        populate_daily_profit()
        firebase_user_update(user_id=users)
    except Exception as e:
        err = ErrorLog.objects.create_log(
        error_description=f"===  ERROR IN UPDATE REALTIME PORTFOLIO FIREBASE ===", error_message=str(e))
        err.send_report_error()
        return {'error':str(e)}
    return {'status':'updated firebase portfolio'}









    