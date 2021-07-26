from django.urls import path
from .views import ClientView,UserClientView,TopStockClientView


urlpatterns = [
    path('', ClientView.as_view(), name='client'),
    path('<str:client_id>/', UserClientView.as_view(), name='client_users'),
    path('<str:client_id>/topstocks/', TopStockClientView.as_view(), name='topstock_client_users'),
]
