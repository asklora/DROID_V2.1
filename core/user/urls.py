from django.urls import path, include
from .views import UserProfile


urlpatterns = [
    path('me/', UserProfile.as_view(), name='profile'),

]
