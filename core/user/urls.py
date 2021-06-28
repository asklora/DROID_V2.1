from django.urls import path
from .views import UserProfile,UserSummaryView


urlpatterns = [
    path('me/', UserProfile.as_view(), name='profile'),
    path('summary/<int:pk>/', UserSummaryView.as_view(), name='summary'),

]