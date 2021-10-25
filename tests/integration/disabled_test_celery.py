import pytest
from config.celery import listener
from core.user.serializers import UserSerializer

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


@pytest.mark.celery(broker_url="memory://")
def test_add_task(celery_app, celery_session_worker):
    @celery_app.task()
    def add(x: int, y: int) -> int:
        return x + y

    assert add.apply((2, 2)).get() == 4


def test_celery_direct_notification(celery_app, celery_worker, user) -> None:
    data = {
        "type": "function",
        "module": "tests.utils.celery.celery_echo",
        "payload": {
            "message": "Hello from celery",
            "status": "success",
        },
    }

    print(user.__dict__);

    task = listener.apply(args=(data,))

    task.get(timeout=10)
    assert task.status == "SUCCESS"
    assert task.get() == data["payload"]


def test_celery_notification(celery_app, celery_worker) -> None:
    data = {
        "type": "function",
        "module": "tests.utils.celery.celery_echo",
        "payload": {
            "message": "Hello from celery",
            "status": "success",
        },
    }

    task = celery_app.send_task(
        "config.celery.listener",
        args=(data,),
        queue="test_celery",
    )

    # this starts the worker, but will lock the session
    # FIXME: find a way to start/stop the worker automatically
    celery_worker.start()

    task.get(timeout=5)
    assert task.status == "SUCCESS"


def test_celery_direct_user(
    celery_app,
    celery_worker,
    mocker,
    user,
) -> None:
    firebase_update = mocker.patch(
        "core.djangomodule.crudlib.user.firebase_user_update",
    )
    daily_profit_update = mocker.patch(
        "core.djangomodule.crudlib.user.populate_daily_profit",
    )

    user_payload = {
        "is_superuser": False,
        "email": "hp-14-notebook-pc-2c826ed5@tests.com",
        "username": "hp-14-notebook-pc-2c826ed5",
        "phone": "012345678",
        "first_name": "Test",
        "last_name": "on hp-14-notebook-pc-2c826ed5",
        "password": user.password,
        "birth_date": None,
        "avatar": None,
        "is_staff": False,
        "is_test": False,
        "is_active": True,
        "date_joined": "2021-10-19T09:14:42.433349",
        "current_status": "verified",
        "is_joined": False,
        "gender": "other",
    }
    print(user_payload)

    data = {
        "type": "function",
        "module": "core.djangomodule.crudlib.user.sync_user",
        "payload": user_payload,
    }

    # task = listener.apply(args=(data,))
    task = celery_app.send_task(
        "config.celery.listener",
        args=(data,),
        queue="test_celery",
    )

    celery_worker.start()

    result = task.get(timeout=10)
    assert task.status == "SUCCESS"
    assert result == UserSerializer(user).data

    assert firebase_update.called()
    assert daily_profit_update.called()
