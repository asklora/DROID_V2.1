from django.contrib import admin
from .models import Order, OrderPosition, PositionPerformance, Feature


admin.site.register(Order)
admin.site.register(OrderPosition)


class PerformanceAdmin(admin.ModelAdmin):
    model = PositionPerformance
    list_filter = (
        "position_uid__ticker",
        "position_uid__ticker__currency_code",
    )

    search_fields = ("performance_uid",)


admin.site.register(PositionPerformance, PerformanceAdmin)
admin.site.register(PositionPerformance, PerformanceAdmin)
admin.site.register(Feature)
