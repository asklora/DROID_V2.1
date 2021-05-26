from django.urls import path
from .views import PositionViews,PositionUserViews,BotPerformanceViews


urlpatterns = [
    path('position/<int:user_id>/', PositionUserViews.as_view({'get':'list'}), name='account_position'),
    path('position/', PositionViews.as_view({'get':'list'}), name='position'),
    path('performance/<str:position_uid>/', BotPerformanceViews.as_view(), name='performance'),
]
