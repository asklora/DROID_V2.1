from django.db import models
from core.universe.models import Universe, Currency
from core.djangomodule.general import generate_id
from core.Clients.models import Client

class DLPAModel(models.Model):
    model_filename = models.TextField(primary_key=True)
    model_type = models.TextField(null=True, blank=True)
    data_period = models.TextField(null=True, blank=True)
    created = models.DateTimeField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    forward_week = models.IntegerField(null=True, blank=True)
    forward_dow = models.TextField(null=True, blank=True)
    train_dow = models.TextField(null=True, blank=True)
    best_train_acc = models.FloatField(null=True, blank=True)
    best_valid_acc = models.FloatField(null=True, blank=True)
    run_time_min = models.FloatField(null=True, blank=True)
    train_num = models.IntegerField(null=True, blank=True)
    cnn_kernel_size = models.IntegerField(null=True, blank=True)
    batch_size = models.IntegerField(null=True, blank=True)
    learning_rate = models.IntegerField(null=True, blank=True)
    lookback = models.IntegerField(null=True, blank=True)
    epoch = models.IntegerField(null=True, blank=True)
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
    num_bins = models.IntegerField(null=True, blank=True)
    num_nans_to_skip = models.IntegerField(null=True, blank=True)
    accuracy_for_embedding = models.IntegerField(null=True, blank=True)
    candle_type_returnsX = models.TextField(null=True, blank=True)
    candle_type_returnsY = models.TextField(null=True, blank=True)
    candle_type_candles = models.TextField(null=True, blank=True)
    seed = models.IntegerField(null=True, blank=True)
    best_valid_epoch = models.IntegerField(null=True, blank=True)
    best_train_epoch = models.IntegerField(null=True, blank=True)
    pc_number = models.TextField(null=True, blank=True)
    stock_percentage = models.FloatField(null=True, blank=True)
    valid_num = models.IntegerField(null=True, blank=True)
    test_num = models.IntegerField(null=True, blank=True)
    num_periods_to_predict = models.IntegerField(null=True, blank=True)

    class Meta:
        managed = True
        db_table = "dlpa_model"

    def __str__(self):
        return f"model_filename : {self.model_filename}"


class DLPAModelStock(models.Model):
    uid = models.CharField(max_length=255, primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code",
                                      related_name="dlpa_model_stock_currency_code", blank=True, null=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker",
                               related_name="dlpa_model_stock_stock_ticker", blank=True, null=True)
    model_filename = models.ForeignKey(DLPAModel, on_delete=models.CASCADE, db_column="model_filename",
                                       related_name="dlpa_model_stock_model_filename", blank=True, null=True)
    data_period = models.TextField(null=True, blank=True)
    created = models.DateTimeField(null=True, blank=True)
    forward_date = models.DateField(null=True, blank=True)
    spot_date = models.DateField(null=True, blank=True)
    year = models.IntegerField(null=True, blank=True)
    week = models.IntegerField(null=True, blank=True)
    day_of_week = models.IntegerField(null=True, blank=True)
    num_periods_to_predict = models.IntegerField(null=True, blank=True)
    number_of_quantiles = models.IntegerField(null=True, blank=True)
    predicted_quantile_1 = models.IntegerField(null=True, blank=True)
    signal_strength_1 = models.FloatField(null=True, blank=True)

    pc_number = models.TextField(null=True, blank=True)

    class Meta:
        managed = True
        db_table = "dlpa_model_stock"

    def __str__(self):
        return f"ticker : {self.ticker.ticker} || model_filename: {self.model_filename}"
