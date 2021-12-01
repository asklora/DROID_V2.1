from config.celery import app
from django.conf import settings
from django.apps import apps
import pandas as pd


def send_to_asklora(payload: dict) -> None:
    app.send_task(
        "config.celery.listener",
        args=(payload,),
        queue=settings.ASKLORA_QUEUE,
    )


def send_notification(username: str, title: str, body: str):

    payload = {
        "type": "function",
        "module": "core.djangomodule.crudlib.notification.send_notif",
        "payload": {
            "username": f"{username}",
            "title": f"{title}",
            "body": f"{body}",
        },
    }
    send_to_asklora(payload)


def send_bulk_notification(title: str, body: str):
    # TODO: need to check bulk message
    payload = {

        "type": "function",
        "module": "core.djangomodule.crudlib.notification.send_notif",
        "payload": {
            "title": f"{title}",
            "body": f"{body}",
        },
    }
    send_to_asklora(payload)



def send_winner_email():
    Season = apps.get_model("user", "Season")
    SeasonHistory = apps.get_model("user", "SeasonHistory")
    last_season = Season.objects.latest("end_date")
    winners = list(
        SeasonHistory.objects.filter(season_id=last_season, rank__gt=0)
        .order_by("rank")
        .values(
            "user_id__email",
            "rank",
            "user_id__first_name",
            "user_id__last_name",
            "user_id__username",
        )
    )
    data = pd.DataFrame(winners)
    data = data.head( len(data) if len(data) < 50 else 50 )
    data = data.rename(
        columns={
            "user_id__email": "email",
            "rank": "ranks",
            "user_id__first_name": "first_name",
            "user_id__last_name": "last_name",
            "user_id__username": "username",
        }
    )

    payload = {
        "type": "function",
        "module": "core.djangomodule.crudlib.winner_email.send_winner_emails",
        "payload": {
            "session": f"{last_season.end_date.year} - {last_season.season_id}",
            "winner": data.to_dict(orient="records"),
        },
    }

    # send_to_asklora(payload=payload)
