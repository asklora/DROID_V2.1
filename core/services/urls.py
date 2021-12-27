from core.services.views import BroadcastSender, Healthcheck
from django.urls import path

urlpatterns = [
    path(
        "broadcast-notification/",
        BroadcastSender.as_view(),
        name="broadcast-notification",
    ),
    path(
        "healthcheck/",
        Healthcheck.as_view(),
        name="healthcheck",
    ),
]
