from django.urls import path
from .views import BotHedgerViews

urlpatterns = [
    path('hedge/', BotHedgerViews.as_view(),name='hedge_bot')
]