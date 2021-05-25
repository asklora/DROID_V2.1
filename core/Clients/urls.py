from django.urls import path
from .views import ClientView,UserClientView


urlpatterns = [
    path('', ClientView.as_view(), name='client'),
    path('<str:client_id>/', UserClientView.as_view(), name='client_users'),
]
