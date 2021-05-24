from django.urls import path
from .views import ClientView


urlpatterns = [
    path('', ClientView.as_view(), name='client'),
]
