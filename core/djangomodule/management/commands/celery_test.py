from config.celery import app
from core.user.models import User
from core.user.serializers import UserSerializer
from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    def handle(self, *args, **options):
        user_payload = {
            "is_superuser": False,
            "email": "hp-14-notebook-pc-2c826ed5@tests.com",
            "username": "hp-14-notebook-pc-2c826ed5",
            "phone": "012345678",
            "first_name": "Test",
            "last_name": "on hp-14-notebook-pc-2c826ed5",
            "password": "pbkdf2_sha256$260000$cdGgPAqgVSd3bqRxjew2f1$+wRo85w0yD1JB6pr0bsU7D6AaxRP1ZnMEJ/jonOmBWk=",
            "birth_date": None,
            "avatar": None,
            "is_staff": False,
            "is_test": False,
            "is_active": True,
            "date_joined": "2021-10-19T09:14:42.433349",
            "current_status": "verified",
            "is_joined": True,
            "gender": "other",
        }
        print(user_payload)

        data = {
            "type": "function",
            "module": "core.djangomodule.crudlib.user.sync_user",
            "payload": user_payload,
        }

        task = app.send_task(
            "config.celery.listener",
            args=(data,),
            queue=settings.UTILS_WORKER_DEFAULT_QUEUE,
        )

        result = task.get(timeout=10)
        assert task.status == "SUCCESS"
        assert result

        user: User = User.objects.get(username=user_payload["username"])
        serialized: dict = UserSerializer(user).data

        assert user

        assert user_payload["email"] == serialized["email"]
        assert user_payload["username"] == serialized["username"]
        assert user_payload["first_name"] == serialized["first_name"]
        assert user_payload["last_name"] == serialized["last_name"]

        assert serialized["balance"] == 100000.0
        assert serialized["current_assets"] == 100000.0

        print(result)
        print(serialized)

        call_command("delete_user", username=user_payload["username"])
