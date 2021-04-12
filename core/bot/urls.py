from django.urls import path, include
from .views import bothedgeview
urlpatterns = [
    path('hedge/', bothedgeview),
]