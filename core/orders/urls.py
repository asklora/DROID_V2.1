from django.urls import path
from .views import (
    PositionViews,
    PositionUserViews,
    BotPerformanceViews,
    OrderViews,
    OrderUpdateViews
    )


urlpatterns = [
    path('position/<int:user_id>/', PositionUserViews.as_view({'get':'list'}), name='account_position'),
    path('position/', PositionViews.as_view({'get':'list'}), name='position'),
    path('performance/<str:position_uid>/', BotPerformanceViews.as_view(), name='performance'),
    path('create/', OrderViews.as_view(), name='order_create'),
    path('update/', OrderUpdateViews.as_view(), name='order_update'),
]
