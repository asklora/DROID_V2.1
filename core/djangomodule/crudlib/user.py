from core.user.models import User,Accountbalance,TransactionHistory,UserDepositHistory
from core.orders.models import OrderPosition,PositionPerformance, Order
from ..general import is_hashed
from django.contrib.auth.hashers import make_password
from ingestion.firestore_migration import firebase_user_update
from bot.calculate_bot import populate_daily_profit
from django.db import IntegrityError
from general.firestore_query import delete_firestore_user
from general.data_process import get_uid
from general.date_process import dateNow

def sync_user(payload):
    create=False
    try:
        user = User.objects.get(username=payload['username'])
    except User.DoesNotExist:
        create =True
    unused_key =[]
    if 'id' in payload:
        user_id =payload.pop('id')
    
    print("CREATING STATUS",create)
    if not create:
        join =payload.get('is_joined',False)
        if user.is_joined:
            payload.pop('is_joined')
        for attrib, val in payload.items():
            if hasattr(user,attrib):
                if attrib == 'password':
                    if is_hashed(payload['password']):
                        setattr(user, attrib,val)
                    else:
                        pwd = make_password(payload['password'])
                        setattr(user, attrib,pwd)
                else:
                    setattr(user, attrib,val)
            else:
                unused_key.append(attrib)
        user.save()
        for key in unused_key:
            payload.pop(key)
        firebase_user_update(user_id=[user.id])
        populate_daily_profit()
        return payload


    parsed_payload ={}
    for attr,val in payload.items():
        if hasattr(User,attr):
            parsed_payload[attr] =val
    try:
        user = User.objects.create(**parsed_payload)
    except IntegrityError:
        return sync_user(payload)
    except Exception as e:
        return {"err": str(e)}
    if user:
        if is_hashed(payload['password']):
            pass
        else:
            user.set_password(payload['password'])
        wallet =Accountbalance.objects.create(user_id=user.id,currency_code_id='HKD',amount=0)
        transaction =TransactionHistory.objects.create(balance_uid=wallet,side='credit',amount=100000,
        transaction_detail={
            'event':'first deposit'
        })
        try:
             deposit_history,created =UserDepositHistory.objects.get_or_create(
                uid = get_uid(user.id, trading_day=dateNow(), replace=True),
                user_id = user,
                trading_day = dateNow(),
                deposit = transaction.amount)
        
        except Exception as e:
            return {"err": str(e)}
        
        user.save()
        parsed_payload['balance_info'] = { 'balance_uid':wallet.balance_uid,'currency_code':'HKD','transaction_id':transaction.id,'transaction_amount':transaction.amount}
        firebase_user_update(user_id=[user.id])
        populate_daily_profit()
        return parsed_payload


def sync_delete_user(payload):
    try:
        user = User.objects.get(username=payload['username'])
        
        PositionPerformance.objects.filter(position_uid__user_id=user).delete()
        Order.objects.filter(user_id=user).delete()
        OrderPosition.objects.filter(user_id=user).delete()
        TransactionHistory.objects.filter(balance_uid__user=user).delete()
        Accountbalance.objects.filter(user=user).delete()
        user_id=user.id
        user.delete()
        delete_firestore_user(user_id)
        
        return {'message':f'{user.username} {user_id} deleted successfully'}

    except User.DoesNotExist:
        return {'message':f'{payload["username"]} doesnt exist, nothing perform'}
    except KeyError:
        return {'message':'payload error'}
    except User.MultipleObjectsReturned:
        User.objects.filter(username=payload['username']).delete()
        return {'message':f'{payload["username"]} found multiple and deleted successfully'}






