from django.contrib import admin
from .models import UniverseConsolidated,Universe
from .forms import AddTicker
from core.services.tasks import get_isin_populate_universe
from import_export.admin import ImportExportModelAdmin
from core.djangomodule.general import generate_id
from import_export import resources
from django.dispatch import receiver
from import_export.signals import post_import, post_export

from django.utils import timezone




class UniverseResource(resources.ModelResource):
    user = None
    class Meta:
        model = UniverseConsolidated
        fields = ('uid','origin_ticker', 'use_isin','use_manual','created','source_id')
        import_id_fields = ('uid',)

    def after_save_instance(self,instance, using_transactions, dry_run, **kwargs):
        try:
            ticker = Universe.objects.get(ticker=instance.origin_ticker)
            get_isin_populate_universe.delay(ticker.ticker,self.user)
            super(UniverseResource, self).after_save_instance(instance, using_transactions, dry_run, **kwargs)
        except Universe.DoesNotExist:
            get_isin_populate_universe.delay(instance.origin_ticker,self.user)
            super(UniverseResource, self).after_save_instance(instance, using_transactions, dry_run, **kwargs)


    def before_import_row(self,row, row_number=None, **kwargs):
        row['uid'] = generate_id(8)
        row['created'] = timezone.now()
        self.user = kwargs['user'].id
        return super(UniverseResource, self).before_import_row(row, **kwargs)



class AddTickerAdmin(ImportExportModelAdmin):
    form = AddTicker
    model = UniverseConsolidated
    resource_class = UniverseResource
    search_fields = ('origin_ticker','consolidated_ticker')
    # ordering = ('email',)

    def save_model( self, request, obj, form, change ):
        #pre save stuff here
        try:
            ticker = Universe.objects.get(ticker=obj.origin_ticker)
            get_isin_populate_universe.delay(ticker.ticker,request.user.id)
            super(AddTickerAdmin, self).save_model(request, obj, form, change)
        except Universe.DoesNotExist:
            obj.save()
            get_isin_populate_universe.delay(obj.origin_ticker,request.user.id)
            super(AddTickerAdmin, self).save_model(request, obj, form, change)
        #post save stuff here
class UniverseAdmin(ImportExportModelAdmin):
    model = Universe
    list_filter = ('created', 'currency_code')


    search_fields = ('ticker',)
admin.site.register(UniverseConsolidated, AddTickerAdmin)
admin.site.register(Universe,UniverseAdmin)
