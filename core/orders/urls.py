from django.urls import path
from .views import (
    PositionViews,
    PositionUserViews,
    BotPerformanceViews,
    OrderViews,
    OrderUpdateViews,
    OrderGetViews,
    OrderActionViews,
    OrderPortfolioCheckView,
    PositionDetailViews
    )


urlpatterns = [
    path('client/position/', PositionViews.as_view({'get':'list'}), name='position'),
    path('position/<int:user_id>/', PositionUserViews.as_view({'get':'list'}), name='account_position'),
    path('position/<str:position_uid>/details/', PositionDetailViews.as_view(), name='account_position_details'),
    path('position/', OrderPortfolioCheckView.as_view(), name='position_check'),
    path('performance/<str:position_uid>/', BotPerformanceViews.as_view(), name='performance'),
    path('create/', OrderViews.as_view(), name='order_create'),
    path('update/', OrderUpdateViews.as_view(), name='order_update'),
    path('get/<str:order_uid>/', OrderGetViews.as_view({'get':'retrieve'}), name='order_detail'),
    path('getall/', OrderGetViews.as_view({'get':'list'}), name='order_list'),
    path('action/', OrderActionViews.as_view(), name='order_action'),
]
