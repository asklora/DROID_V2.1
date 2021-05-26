from django.urls import path
from .views import PositionViews,PositionUserViews


urlpatterns = [
    path('position/<int:user_id>/', PositionUserViews.as_view({'get':'list'}), name='account_position'),
    path('position/', PositionViews.as_view({'get':'list'}), name='position'),
]
