import pytest
from config.celery import listener
from django.core.management import call_command

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
