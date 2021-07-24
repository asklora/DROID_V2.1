from core.user.models import User,Accountbalance,TransactionHistory
from ..general import is_hashed
from django.contrib.auth.hashers import make_password


def sync_user(payload):
    create=False
    try:
        user = User.objects.get(username=payload['username'])
    except User.DoesNotExist:
        create =True
    unused_key =[]
    if not create:
        if 'id' in payload:
            payload.pop('id')
        
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

        return payload


    parsed_payload ={}
    for attr,val in payload.items():
        if hasattr(User,attr):
            parsed_payload[attr] =val
    user = User.objects.create(**parsed_payload)
    if user:
        if is_hashed(payload['password']):
            pass
        else:
            user.set_password(payload['password'])
        user.save()
        wallet =Accountbalance.objects.create(user_id=user.id,currency_code_id='HKD',amount=0)
        transaction =TransactionHistory.objects.create(balance_uid=wallet,side='credit',amount=100000,
        transaction_detail={
            'event':'first deposit'
        })
        parsed_payload['balance_info'] = { 'balance_uid':wallet.balance_uid,'currency_code':'HKD','transaction_id':transaction.id,'transaction_amount':transaction.amount}
        return parsed_payload


def sync_delete_user(payload):
    try:
        user = User.objects.get(username=payload['username'])
        user.delete()
        return {'message':f'{user.username} deleted successfully'}

    except User.DoesNotExist:
        return {'message':f'{payload["username"]} doesnt exist, nothing perform'}
    except KeyError:
        return {'message':'payload error'}
    except User.MultipleObjectsReturned:
        User.objects.filter(username=payload['username']).delete()
        return {'message':f'{payload["username"]} found multiple and deleted successfully'}






