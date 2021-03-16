from django.contrib import admin
from .models import UniverseConsolidated,Universe
from .forms import AddTicker
from core.services.tasks import get_isin_populate_universe
from import_export.admin import ImportExportModelAdmin
from core.djangomodule.general import generate_id
from import_export import resources
from django.dispatch import receiver
from django.utils import timezone
from django.shortcuts import HttpResponseRedirect,reverse


class UniverseResource(resources.ModelResource):
    user = None
    ticker = []

    class Meta:
        model = UniverseConsolidated
        fields = ('uid','origin_ticker', 'use_isin','use_manual','created','source_id')
        import_id_fields = ('uid',)
    
    
    def after_save_instance(self, instance, using_transactions, dry_run):
        """
        Override to add additional logic. Does nothing by default.
        """
        if not dry_run:
            instance.save()
            self.ticker.append(instance.origin_ticker)
            duplicate = UniverseConsolidated.objects.filter(
            origin_ticker=instance.origin_ticker, 
            use_isin=instance.use_isin,
            use_manual=instance.use_manual
        )
            try:
                ticker = Universe.objects.get(ticker=instance.origin_ticker)
                if duplicate.count() >= 1:
                    instance.delete()
                # get_isin_populate_universe.delay(ticker.ticker,self.user)
            except Universe.DoesNotExist:
                if duplicate.count() >= 1:
                    # existing_ticker=duplicate.first().origin_ticker
                    instance.delete()
                    # get_isin_populate_universe.delay(existing_ticker,self.user)
                # else:
                    # get_isin_populate_universe.delay(instance.origin_ticker,self.user)
        else:
            # self.ticker.append(instance.origin_ticker)
            instance.delete()



    def before_import_row(self,row, row_number=None, **kwargs):
        row['uid'] = generate_id(8)
        row['created'] = timezone.now()
        self.user = kwargs['user'].id
        # return super(UniverseResource, self).before_import_row(row, **kwargs)



class AddTickerAdmin(ImportExportModelAdmin):
    form = AddTicker
    model = UniverseConsolidated
    resource_class = UniverseResource
    search_fields = ('origin_ticker','consolidated_ticker')
    # ordering = ('email',)
    def process_result(self, result, request):
        self.generate_log_entries(result, request)
        self.add_success_message(result, request)
        resources = self.get_import_resource_class()
        # post_import.send(sender=None, model=self.model)
        get_isin_populate_universe.delay(resources.ticker,request.user.id)
        # print(self.get_import_resource_class().ticker)
        url = reverse('admin:%s_%s_changelist' % self.get_model_info(),
                      current_app=self.admin_site.name)
        return HttpResponseRedirect(url)
    def save_model( self, request, obj, form, change ):
        #pre save stuff here
        try:
            ticker = Universe.objects.get(ticker=obj.origin_ticker)
            get_isin_populate_universe.delay(ticker.ticker,request.user.id)
            return super(AddTickerAdmin, self).save_model(request, obj, form, change)
        except Universe.DoesNotExist:
            obj.save()
            get_isin_populate_universe.delay(obj.origin_ticker,request.user.id)
            return super(AddTickerAdmin, self).save_model(request, obj, form, change)
        #post save stuff here
class UniverseAdmin(ImportExportModelAdmin):
    model = Universe
    list_filter = ('created', 'currency_code')


    search_fields = ('ticker',)
admin.site.register(UniverseConsolidated, AddTickerAdmin)
admin.site.register(Universe,UniverseAdmin)
