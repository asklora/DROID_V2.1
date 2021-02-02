from django.db import models
from core.universe.models import Universe,Currency,IntegrityError
from core.djangomodule.models import BaseTimeStampModel
from core.djangomodule.general import generate_id
from core.Clients.models import Client

class Subinterval(models.Model):
    subinterval = models.PositiveIntegerField(primary_key=True)
    spot_date = models.DateField(null=True, blank=True)
    date_initial = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'subinterval'

    def __str__(self):
        interval = str(self.subinterval)
        return interval

    def save(self):
        iso_calendar = self.spot_date.isocalendar()
        self.subinterval = int(f'{iso_calendar[0]}{iso_calendar[1]}')
        self.date_initial = self.spot_date.replace('-', '')
        super(Subinterval, self).save()


class IndexPerformance(models.Model):
    uid = models.CharField(primary_key=True, unique=True,
                           editable=False, max_length=200)
    index_currency= models.ForeignKey(
        Currency, on_delete=models.CASCADE, related_name='index_currency_performance', db_column='index_currency')
    subinterval = models.ForeignKey(
        Subinterval, on_delete=models.CASCADE, related_name='weekdata_performance')
    types = models.CharField(max_length=200, null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    index_spot_price = models.FloatField(null=True, blank=True)
    index_forward_price = models.FloatField(null=True, blank=True)
    index_forward_return = models.FloatField(null=True, blank=True)
    wts_tri = models.FloatField(null=True, blank=True)  # Performance
    index_tri = models.FloatField(null=True, blank=True)  # Benchmark
    average_forward_return = models.FloatField(null=True, blank=True)
    week_status = models.CharField(max_length=200, null=True, blank=True)

    class Meta:
        db_table = 'top_stock_performance'

    def __str__(self):
        return f'type : {self.index_currency.currency_code} || ticker: {self.spot_date}'

    ### Benchmark

    def normalizeindextri(self):
        result = self.index_tri
        result = round(result, 2)
        return result
     ### Performance

    def normalizeweeklyperf(self):
        result = self.wts_tri
        result = round(result, 2)
        return result

class WeeklyTopStock(models.Model):
    uid = models.CharField(primary_key=True, unique=True,
                           editable=False, max_length=200)
    subinterval = models.ForeignKey(
        Subinterval, on_delete=models.CASCADE, related_name='weekdata_topstock')
    ticker = models.ForeignKey(
        Universe, on_delete=models.CASCADE, related_name='weekly_top_stock')
    spot_date = models.DateField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    rank = models.IntegerField(null=True, blank=True)
    types = models.CharField(max_length=50, null=True, blank=True)
    prediction_period = models.CharField(max_length=200, null=True, blank=True)
    spot_price = models.FloatField(null=True, blank=True)
    spot_tri = models.FloatField(null=True, blank=True)
    forward_tri = models.FloatField(null=True, blank=True)
    forward_return = models.FloatField(
        null=True, blank=True, verbose_name='absolute_return')
    index_forward_return = models.FloatField(
        null=True, blank=True, verbose_name='ewportperfomance')

    class Meta:
        db_table = 'top_stock_weekly'

    @property
    def absoluteperf(self):
        if self.forward_return == None:
            self.forward_return = 0
        absolute = self.forward_return * 100
        absolute = round(absolute, 2)
        return absolute

    @property
    def perfvsbenchmark(self):
        if self.forward_return == None:
            self.forward_return = 0
        if self.index_forward_return == None:
            self.index_forward_return = 0
        perfvsbench = self.forward_return - self.index_forward_return
        perfvsbench = perfvsbench * 100
        perfvsbench = round(perfvsbench, 2)

        return perfvsbench

    def __str__(self):
        return f'type : {self.types} || ticker: {self.ticker}'






