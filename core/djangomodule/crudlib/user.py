from core.user.models import User


def sync_user(payload):
    try:
        user = User.objects.get(id=payload['id'])
        for attrib, val in payload.items():
            setattr(user, attrib,val)
        user.save()
    except User.DoesNotExist:
        user = User.objects.create(**payload)
        if user:
            user.set_password(payload['password'])
            user.save()
            print(user,'created')
    
    print(user)