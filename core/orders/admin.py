from django.contrib import admin
from .models import Order,OrderPosition,PositionPerformance


admin.site.register(Order)
admin.site.register(OrderPosition)
admin.site.register(PositionPerformance)

