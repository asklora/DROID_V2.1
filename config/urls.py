from django.contrib import admin
from django.urls import path, include,re_path
from rest_framework import permissions
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView, SpectacularSwaggerView
from rest_framework_simplejwt.views import (
    TokenRefreshView,
    TokenVerifyView
)
from core.user.views import RevokeToken,PairTokenView
from .views import IndexWs



urlpatterns = [
    path('admin/', admin.site.urls,name='adminlogin'),
    path('login/', admin.site.login, name='default-admin-login'),
    path('swagger/', SpectacularAPIView.as_view(), name='schema'),
    path('listener/', IndexWs.as_view(), name='listener'),
    # Optional UI cahnge
    path('docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    path('api/auth/', PairTokenView.as_view(), name='token_obtain_pair'),
    path('api/auth/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('api/auth/verify/', TokenVerifyView.as_view(), name='token_verify'),
    path('api/auth/revoke/', RevokeToken.as_view(), name='token_revoke'),
    path('api/user/',include('core.user.urls')),
    path('api/client/',include('core.Clients.urls')),
    path('api/order/',include('core.orders.urls')),
]
