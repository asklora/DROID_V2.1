from django.contrib import admin
from .models import UniverseConsolidated,Universe
from .forms import AddTicker
from core.services.tasks import get_isin_populate_universe




class AppUserAdmin(admin.ModelAdmin):
    form = AddTicker
    model = UniverseConsolidated

    search_fields = ('origin_ticker','consolidated_ticker')
    # ordering = ('email',)

    def save_model( self, request, obj, form, change ):
        #pre save stuff here
        try:
            ticker = Universe.objects.get(ticker=obj.origin_ticker)
            get_isin_populate_universe.delay(ticker.ticker,request.user.id)
            super(AppUserAdmin, self).save_model(request, obj, form, change)
        except Universe.DoesNotExist:
            obj.save()
            get_isin_populate_universe.delay(obj.origin_ticker,request.user.id)
            super(AppUserAdmin, self).save_model(request, obj, form, change)
        #post save stuff here
admin.site.register(UniverseConsolidated, AppUserAdmin)
