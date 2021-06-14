from core.user.models import User
from ..general import is_hashed
from django.contrib.auth.hashers import make_password


def sync_user(payload):
    try:
        user = User.objects.get(id=payload['id'])
        for attrib, val in payload.items():
            if attrib == 'password':
                if is_hashed(payload['password']):
                    setattr(user, attrib,val)
                else:
                    pwd = make_password(payload['password'])
                    setattr(user, attrib,pwd)
            else:
                setattr(user, attrib,val)
        user.save()
    except User.DoesNotExist:
        user = User.objects.create(**payload)
        if user:
            if is_hashed(payload['password']):
                user.password =payload['password']
            else:
                user.set_password(payload['password'])
            user.save()
            print(user,'created')
    
    print(user)