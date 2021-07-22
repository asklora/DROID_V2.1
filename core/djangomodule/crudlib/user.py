from core.user.models import User
from ..general import is_hashed
from django.contrib.auth.hashers import make_password


def sync_user(payload):
    try:
        if 'id' in payload:
            payload.pop('id')
        create=False
        user = User.objects.get(username=payload['username'])
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
        user.save()
    except User.DoesNotExist:
        create =True
    if not create:
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
        return payload
    