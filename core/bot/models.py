from core.universe.models import Currency, Universe
from django.db import models

# Create your models here.
class BotBacktest(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_backtest_ticker")
    spot_date = models.DateField(blank=True, null=True)
    bot_type = models.TextField(blank=True, null=True)
    option_type = models.TextField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    spot_price = models.FloatField(blank=True, null=True)
    potential_max_loss = models.FloatField(blank=True, null=True)
    targeted_profit = models.FloatField(blank=True, null=True)
    bot_return = models.FloatField(blank=True, null=True)
    event = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "bot_backtest"

    def __str__(self):
        return self.ticker

class ClassicBacktest(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_classic_backtest_ticker")
    spot_date = models.DateField(blank=True, null=True)
    classic_vol = models.FloatField(blank=True, null=True)
    spot_price = models.FloatField(blank=True, null=True)
    vol_period = models.FloatField(blank=True, null=True)
    stop_loss = models.FloatField(blank=True, null=True)
    take_profit = models.FloatField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    expiry_date = models.DateField(blank=True, null=True)
    event_date = models.DateField(blank=True, null=True)
    event_price = models.FloatField(blank=True, null=True)
    expiry_price = models.FloatField(blank=True, null=True)
    event = models.TextField(blank=True, null=True)
    bot_return = models.FloatField(blank=True, null=True)
    expiry_return = models.FloatField(blank=True, null=True)
    duration = models.FloatField(blank=True, null=True)
    drawdown_return = models.FloatField(blank=True, null=True)
    pnl = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "bot_classic_backtest"

    def __str__(self):
        return self.ticker

class UnoBacktest(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_uno_backtest_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="bot_uno_backtest_currency_code")
    now_date = models.DateField(blank=True, null=True)
    spot_date = models.DateField(blank=True, null=True)
    now_price = models.FloatField(blank=True, null=True)
    spot_price = models.FloatField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    expiry_date = models.DateField(blank=True, null=True)
    days_to_expiry = models.IntegerField(blank=True, null=True)
    r = models.FloatField(blank=True, null=True)
    q = models.FloatField(blank=True, null=True)
    t = models.IntegerField(blank=True, null=True)
    vol_t = models.FloatField(blank=True, null=True)
    option_type = models.TextField(blank=True, null=True)
    strike = models.FloatField(blank=True, null=True)
    barrier = models.FloatField(blank=True, null=True)
    v1 = models.FloatField(blank=True, null=True)
    v2 = models.FloatField(blank=True, null=True)
    target_max_loss = models.FloatField(blank=True, null=True)
    stock_balance = models.FloatField(blank=True, null=True)
    stock_price = models.FloatField(blank=True, null=True)
    event_date = models.DateField(blank=True, null=True)
    event_price = models.FloatField(blank=True, null=True)
    event = models.TextField(blank=True, null=True)
    pnl = models.FloatField(blank=True, null=True)
    expiry_payoff = models.FloatField(blank=True, null=True)
    expiry_return = models.FloatField(blank=True, null=True)
    expiry_price = models.FloatField(blank=True, null=True)
    drawdown_return = models.FloatField(blank=True, null=True)
    duration = models.FloatField(blank=True, null=True)
    bot_return = models.FloatField(blank=True, null=True)
    inferred = models.IntegerField(blank=True, null=True)
    target_profit = models.FloatField(blank=True, null=True)
    delta_churn = models.FloatField(blank=True, null=True)
    modify_arg = models.TextField(blank=True, null=True)
    modified = models.IntegerField(blank=True, null=True)
    num_hedges = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "bot_uno_backtest"

    def __str__(self):
        return self.ticker

class UcdcBacktest(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_ucdc_backtest_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="bot_ucdc_backtest_currency_code")
    now_date = models.DateField(blank=True, null=True)
    spot_date = models.DateField(blank=True, null=True)
    now_price = models.FloatField(blank=True, null=True)
    spot_price = models.FloatField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    expiry_date = models.DateField(blank=True, null=True)
    days_to_expiry = models.IntegerField(blank=True, null=True)
    r = models.FloatField(blank=True, null=True)
    q = models.FloatField(blank=True, null=True)
    t = models.IntegerField(blank=True, null=True)
    vol_t = models.FloatField(blank=True, null=True)
    option_type = models.TextField(blank=True, null=True)
    strike_1_type = models.TextField(blank=True, null=True)
    strike_2_type = models.TextField(blank=True, null=True)
    strike_1 = models.FloatField(blank=True, null=True)
    strike_2 = models.FloatField(blank=True, null=True)
    v1 = models.FloatField(blank=True, null=True)
    v2 = models.FloatField(blank=True, null=True)
    target_profit = models.FloatField(blank=True, null=True)
    target_max_loss = models.FloatField(blank=True, null=True)
    stock_balance = models.FloatField(blank=True, null=True)
    stock_price = models.FloatField(blank=True, null=True)
    event_date = models.DateField(blank=True, null=True)
    event_price = models.FloatField(blank=True, null=True)
    event = models.TextField(blank=True, null=True)
    pnl = models.FloatField(blank=True, null=True)
    delta_churn = models.FloatField(blank=True, null=True)
    expiry_payoff = models.FloatField(blank=True, null=True)
    expiry_return = models.FloatField(blank=True, null=True)
    expiry_price = models.FloatField(blank=True, null=True)
    drawdown_return = models.FloatField(blank=True, null=True)
    duration = models.FloatField(blank=True, null=True)
    bot_return = models.FloatField(blank=True, null=True)
    inferred = models.IntegerField(blank=True, null=True)
    modified = models.IntegerField(blank=True, null=True)
    modify_arg = models.TextField(blank=True, null=True)
    num_hedges = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "bot_ucdc_backtest"

    def __str__(self):
        return self.ticker
class BotData(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_data_ticker")
    trading_day = models.DateField(blank=True, null=True)
    c2c_vol_0_21 = models.FloatField(blank=True, null=True)
    c2c_vol_21_42 = models.FloatField(blank=True, null=True)
    c2c_vol_42_63 = models.FloatField(blank=True, null=True)
    c2c_vol_63_126 = models.FloatField(blank=True, null=True)
    c2c_vol_126_252 = models.FloatField(blank=True, null=True)
    c2c_vol_252_504 = models.FloatField(blank=True, null=True)
    kurt_0_504 = models.FloatField(blank=True, null=True)
    rs_vol_0_21 = models.FloatField(blank=True, null=True)
    rs_vol_21_42 = models.FloatField(blank=True, null=True)
    rs_vol_42_63 = models.FloatField(blank=True, null=True)
    rs_vol_63_126 = models.FloatField(blank=True, null=True)
    rs_vol_126_252 = models.FloatField(blank=True, null=True)
    rs_vol_252_504 = models.FloatField(blank=True, null=True)
    total_returns_0_1 = models.FloatField(blank=True, null=True)
    total_returns_0_21 = models.FloatField(blank=True, null=True)
    total_returns_0_63 = models.FloatField(blank=True, null=True)
    total_returns_21_126 = models.FloatField(blank=True, null=True)
    total_returns_21_231 = models.FloatField(blank=True, null=True)
    vix_value = models.FloatField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    atm_volatility_spot_x = models.FloatField(blank=True, null=True)
    atm_volatility_one_year_x = models.FloatField(blank=True, null=True)
    atm_volatility_infinity_x = models.FloatField(blank=True, null=True)
    total_returns_0_63_x = models.FloatField(blank=True, null=True)
    total_returns_21_126_x = models.FloatField(blank=True, null=True)
    total_returns_0_21_x = models.FloatField(blank=True, null=True)
    total_returns_21_231_x = models.FloatField(blank=True, null=True)
    c2c_vol_0_21_x = models.FloatField(blank=True, null=True)
    c2c_vol_21_42_x = models.FloatField(blank=True, null=True)
    c2c_vol_42_63_x = models.FloatField(blank=True, null=True)
    c2c_vol_63_126_x = models.FloatField(blank=True, null=True)
    c2c_vol_126_252_x = models.FloatField(blank=True, null=True)
    c2c_vol_252_504_x = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "bot_data"

    def __str__(self):
        return self.ticker

class VolSurface(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="data_vol_surface_ticker")
    trading_day = models.DateField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "data_vol_surface"

    def __str__(self):
        return self.ticker

class VolSurfaceInferred(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="data_vol_surface_inferred_ticker")
    trading_day = models.DateField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "data_vol_surface_inferred"

    def __str__(self):
        return self.ticker