from django.db import models
from core.universe.models import Universe, Currency
from core.djangomodule.general import generate_id
from core.Clients.models import Client

class TopStockModel(models.Model):
    model_filename = models.TextField(primary_key=True)
    model_type = models.TextField(null=True, blank=True)
    data_period = models.TextField(null=True, blank=True)
    created = models.DateField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    forward_week = models.IntegerField(null=True,blank=True)
    forward_dow = models.TextField(null=True, blank=True)
    train_dow = models.TextField(null=True, blank=True)
    best_train_acc = models.FloatField(null=True, blank=True)
    best_valid_acc = models.FloatField(null=True, blank=True)
    test_acc_1 = models.FloatField(null=True, blank=True)
    test_acc_2 = models.FloatField(null=True, blank=True)
    test_acc_3 = models.FloatField(null=True, blank=True)
    test_acc_4 = models.FloatField(null=True, blank=True)
    test_acc_5 = models.FloatField(null=True, blank=True)
    run_time_min = models.FloatField(null=True, blank=True)
    train_num = models.IntegerField(null=True,blank=True)
    cnn_kernel_size = models.IntegerField(null=True,blank=True)
    batch_size = models.IntegerField(null=True,blank=True)
    learning_rate = models.IntegerField(null=True,blank=True)
    lookback = models.IntegerField(null=True,blank=True)
    epoch = models.IntegerField(null=True,blank=True)
    param_name_1 = models.TextField(null=True, blank=True)
    param_val_1 = models.FloatField(null=True, blank=True)
    param_name_2 = models.TextField(null=True, blank=True)
    param_val_2 = models.FloatField(null=True, blank=True)
    param_name_3 = models.TextField(null=True, blank=True)
    param_val_3 = models.FloatField(null=True, blank=True)
    param_name_4 = models.TextField(null=True, blank=True)
    param_val_4 = models.TextField(null=True, blank=True)
    param_name_5 = models.TextField(null=True, blank=True)
    param_val_5 = models.TextField(null=True, blank=True)
    num_bins = models.IntegerField(null=True,blank=True)
    num_nans_to_skip = models.IntegerField(null=True,blank=True)
    accuracy_for_embedding = models.IntegerField(null=True,blank=True)
    candle_type_returnsX = models.TextField(null=True, blank=True)
    candle_type_returnsY = models.TextField(null=True, blank=True)
    candle_type_candles = models.TextField(null=True, blank=True)
    seed = models.IntegerField(null=True,blank=True)
    best_valid_epoch = models.IntegerField(null=True,blank=True)
    best_train_epoch = models.IntegerField(null=True,blank=True)
    pc_number = models.TextField(null=True, blank=True)
    stock_percentage = models.FloatField(null=True, blank=True)
    valid_num = models.IntegerField(null=True,blank=True)
    test_num = models.IntegerField(null=True,blank=True)
    num_periods_to_predict = models.IntegerField(null=True,blank=True)
    should_use = models.BooleanField(null=True,blank=True, default=False)
    long_term = models.BooleanField(null=True,blank=True, default=False)
    train_len = models.IntegerField(null=True,blank=True)
    valid_len = models.IntegerField(null=True,blank=True)

    class Meta:
        db_table = "top_stock_models"

    def __str__(self):
        return f"model_filename: {self.model_filename}"


class TopStockModelStock(models.Model):
    uid = models.CharField(max_length=255,primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="top_stock_models_stock_currency_code", blank=True, null=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="top_stock_models_stock_ticker", blank=True, null=True)
    model_filename = models.ForeignKey(TopStockModel, on_delete=models.CASCADE, db_column="model_filename", related_name="top_stock_models_stock_model_filename", blank=True, null=True)
    data_period = models.TextField(null=True, blank=True)
    created = models.DateField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    year = models.IntegerField(null=True,blank=True)
    week = models.IntegerField(null=True,blank=True)
    day_of_week = models.IntegerField(null=True,blank=True)
    num_periods_to_predict = models.IntegerField(null=True,blank=True)
    number_of_quantiles = models.IntegerField(null=True,blank=True)
    predicted_quantile_1 = models.IntegerField(null=True,blank=True)
    signal_strength_1 = models.FloatField(null=True, blank=True)
    
    pc_number = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "top_stock_models_stock"

    def __str__(self):
        return f"type : {self.index_currency.currency_code} || ticker: {self.spot_date}"


class Subinterval(models.Model):
    subinterval = models.PositiveIntegerField(primary_key=True)
    spot_date = models.DateField(null=True, blank=True)
    date_initial = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = "subinterval"

    def __str__(self):
        interval = str(self.subinterval)
        return interval

    def save(self):
        iso_calendar = self.spot_date.isocalendar()
        self.subinterval = int(f"{iso_calendar[0]}{iso_calendar[1]}")
        self.date_initial = self.spot_date.replace("-", "")
        super(Subinterval, self).save()


class TopStockWeekly(models.Model):
    uid = models.CharField(primary_key=True, unique=True,editable=False, max_length=200)
    subinterval = models.ForeignKey(Subinterval, on_delete=models.CASCADE, related_name="top_stock_weekly_subinterval")
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, related_name="top_stock_weekly_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="top_stock_weekly_currency_code")
    client_id = models.ForeignKey(Client, on_delete=models.CASCADE, db_column="client_id", related_name="top_stock_weekly_client_id")
    created = models.DateField(null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    rank = models.IntegerField(null=True, blank=True)
    types = models.CharField(max_length=50, null=True, blank=True)
    prediction_period = models.CharField(max_length=200, null=True, blank=True)
    spot_price = models.FloatField(null=True, blank=True)
    spot_tri = models.FloatField(null=True, blank=True)
    forward_tri = models.FloatField(null=True, blank=True)
    forward_return = models.FloatField(null=True, blank=True, verbose_name="absolute_return")
    index_forward_return = models.FloatField(null=True, blank=True, verbose_name="ewportperfomance")
    index_spot_price = models.FloatField(null=True, blank=True)
    index_forward_price = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "top_stock_weekly"

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
        return f"type : {self.types} || ticker: {self.ticker}"


class TopStockPerformance(models.Model):
    uid = models.CharField(primary_key=True, unique=True,editable=False, max_length=200)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="top_stock_performance_currency_code")
    subinterval = models.ForeignKey(Subinterval, on_delete=models.CASCADE, db_column="subinterval", related_name="top_stock_performance_subinterval")
    client_id = models.ForeignKey(Client, on_delete=models.CASCADE, db_column="client_id", related_name="top_stock_performance_client_id")
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
        db_table = "top_stock_performance"

    def __str__(self):
        return f"type : {self.index_currency.currency_code} || ticker: {self.spot_date}"

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


# class TopStockLog(BaseTimeStampModel):
#     year = models.IntegerField()
#     week = models.IntegerField()
#     currency_code = models.CharField(max_length=5)
#     is_ordered = models.BooleanField(default=False)
#     order_date = models.DateTimeField(null=True, blank=True)
#     client_id = models.CharField(max_length=50)


#     def __str__(self):
#         return self.client_id

#     class Meta:
#         db_table = "top_stock_log"
#         ordering = ['year','week']

