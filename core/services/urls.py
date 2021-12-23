from core.services.views import BroadcastSender
from django.urls import path

urlpatterns = [
    path(
        "broadcast-notification/",
        BroadcastSender.as_view(),
        name="broadcast-notification",
    ),
]
