from core.universe.models import Currency, Universe
from django.db import models

# Create your models here.
class BotType(models.Model):
    bot_type = models.TextField(primary_key=True)
    bot_name = models.TextField(blank=True, null=True)
    bot_apps_name = models.TextField(blank=True, null=True)
    bot_apps_description= models.TextField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "bot_type"

    def __str__(self):
        return f"{self.bot_type}"

class BotOptionType(models.Model):
    bot_id = models.TextField(primary_key=True)
    bot_type = models.ForeignKey(BotType, on_delete=models.CASCADE, db_column="bot_type", related_name="bot_option_type_bot_type", null=True)
    bot_option_type = models.TextField(blank=True, null=True)
    bot_option_name = models.TextField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    time_to_exp_str = models.TextField(blank=True, null=True)
    duration = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "bot_option_type"
        
    def is_uno(self):
        return self.bot_type.bot_type == 'UNO'
    
    def is_classic(self):
        return self.bot_type.bot_type == 'CLASSIC'
    
    def is_ucdc(self):
        return self.bot_type.bot_type == 'UCDC'

    def __str__(self):
        return f"{self.bot_type}-{self.bot_option_type}-{self.time_to_exp}"

class BotBacktest(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_backtest_ticker")
    bot_id = models.ForeignKey(BotOptionType, on_delete=models.CASCADE, db_column="bot_id", related_name="lbot_backtest_bot_id")
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
        return self.ticker.ticker

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
        return self.ticker.ticker

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
        return self.ticker.ticker

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
        return self.ticker.ticker


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
        return self.ticker.ticker

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
        return self.ticker.ticker

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
        return self.ticker.ticker

class LatestVol(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE,db_column="ticker", related_name="latest_vol_ticker", primary_key=True)
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
        db_table = "latest_vol"

    def __str__(self):
        return self.ticker.ticker

class BotStatistic(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_statistic_ticker")
    option_type = models.TextField(blank=True, null=True)
    bot_type = models.TextField(blank=True, null=True)
    lookback = models.BigIntegerField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    avg_days = models.FloatField(blank=True, null=True)
    pct_profit = models.FloatField(blank=True, null=True)
    pct_losses = models.FloatField(blank=True, null=True)
    avg_profit = models.FloatField(blank=True, null=True)
    avg_loss = models.FloatField(blank=True, null=True)
    avg_return = models.FloatField(blank=True, null=True)
    pct_max_profit = models.FloatField(blank=True, null=True)
    pct_max_loss = models.FloatField(blank=True, null=True)
    ann_avg_return = models.FloatField(blank=True, null=True)
    ann_avg_return_bm = models.FloatField(blank=True, null=True)
    avg_return_bm = models.FloatField(blank=True, null=True)
    max_loss_bot = models.FloatField(blank=True, null=True)
    max_loss_bm = models.FloatField(blank=True, null=True)
    avg_days_max_profit = models.FloatField(blank=True, null=True)
    avg_days_max_loss = models.FloatField(blank=True, null=True)
    
    class Meta:
        managed = True
        db_table = "bot_statistic"

    def __str__(self):
        return f"{self.ticker.ticker}-{self.bot_type}"
    
class BotRanking(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="bot_ranking_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="bot_ranking_currency_code")
    spot_date = models.DateField(blank=True, null=True)
    model_type = models.TextField(blank=True, null=True)
    created = models.DateField(blank=True, null=True)
    uno_OTM_003846_pnl_class_prob	 = models.FloatField(blank=True, null=True)
    uno_OTM_007692_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_OTM_008333_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_OTM_015384_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_OTM_016666_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_OTM_025_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_OTM_05_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_003846_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_007692_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_008333_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_015384_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_016666_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_025_pnl_class_prob = models.FloatField(blank=True, null=True)
    uno_ITM_05_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_003846_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_007692_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_008333_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_015384_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_016666_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_025_pnl_class_prob = models.FloatField(blank=True, null=True)
    ucdc_ATM_05_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_003846_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_007692_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_008333_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_015384_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_016666_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_025_pnl_class_prob = models.FloatField(blank=True, null=True)
    classic_classic_05_pnl_class_prob = models.FloatField(blank=True, null=True)
    rank_1_003846 = models.TextField(blank=True, null=True)
    rank_2_003846 = models.TextField(blank=True, null=True)
    rank_3_003846 = models.TextField(blank=True, null=True)
    rank_4_003846 = models.TextField(blank=True, null=True)
    rank_1_007692 = models.TextField(blank=True, null=True)
    rank_2_007692 = models.TextField(blank=True, null=True)
    rank_3_007692 = models.TextField(blank=True, null=True)
    rank_4_007692 = models.TextField(blank=True, null=True)
    rank_1_008333 = models.TextField(blank=True, null=True)
    rank_2_008333 = models.TextField(blank=True, null=True)
    rank_3_008333 = models.TextField(blank=True, null=True)
    rank_4_008333 = models.TextField(blank=True, null=True)
    rank_1_015384 = models.TextField(blank=True, null=True)
    rank_2_015384 = models.TextField(blank=True, null=True)
    rank_3_015384 = models.TextField(blank=True, null=True)
    rank_4_015384 = models.TextField(blank=True, null=True)
    rank_1_016666 = models.TextField(blank=True, null=True)
    rank_2_016666 = models.TextField(blank=True, null=True)
    rank_3_016666 = models.TextField(blank=True, null=True)
    rank_4_016666 = models.TextField(blank=True, null=True)
    rank_1_025 = models.TextField(blank=True, null=True)
    rank_2_025 = models.TextField(blank=True, null=True)
    rank_3_025 = models.TextField(blank=True, null=True)
    rank_4_025 = models.TextField(blank=True, null=True)
    rank_1_05 = models.TextField(blank=True, null=True)
    rank_2_05 = models.TextField(blank=True, null=True)
    rank_3_05 = models.TextField(blank=True, null=True)
    rank_4_05 = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "bot_ranking"

    def __str__(self):
        return f"{self.ticker.ticker}-{self.spot_date}"


class LatestBotRanking(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="latest_bot_ranking_ticker")
    bot_id = models.ForeignKey(BotOptionType, on_delete=models.CASCADE, db_column="bot_id", related_name="latest_bot_ranking_bot_id")
    spot_date = models.DateField(blank=True, null=True)
    bot_type = models.TextField(blank=True, null=True)
    bot_option_type = models.TextField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    time_to_exp_str = models.TextField(blank=True, null=True)
    ranking = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "latest_bot_ranking"

    def __str__(self):
        return f"{self.ticker.ticker}-{self.bot_id}"

class LatestBotUpdate(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="latest_bot_update_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="latest_bot_update_currency_code")
    bot_id = models.ForeignKey(BotOptionType, on_delete=models.CASCADE, db_column="bot_id", related_name="latest_bot_update_bot_id")
    spot_price = models.FloatField(blank=True, null=True)
    spot_date = models.DateField(blank=True, null=True)
    potential_max_loss = models.FloatField(blank=True, null=True)
    targeted_profit = models.FloatField(blank=True, null=True)
    bot_type = models.TextField(blank=True, null=True)
    bot_option_type = models.TextField(blank=True, null=True)
    time_to_exp = models.FloatField(blank=True, null=True)
    time_to_exp_str = models.TextField(blank=True, null=True)
    v0 = models.FloatField(blank=True, null=True)
    vol_t_003846 = models.FloatField(blank=True, null=True)
    vol_t_007692 = models.FloatField(blank=True, null=True)
    vol_t_008333 = models.FloatField(blank=True, null=True)
    vol_t_015384 = models.FloatField(blank=True, null=True)
    vol_t_016666 = models.FloatField(blank=True, null=True)
    vol_t_025 = models.FloatField(blank=True, null=True)
    vol_t_05 = models.FloatField(blank=True, null=True)
    classic_vol = models.FloatField(blank=True, null=True)
    option_price = models.FloatField(blank=True, null=True)
    strike = models.FloatField(blank=True, null=True)
    strike_2 = models.FloatField(blank=True, null=True)
    barrier = models.FloatField(blank=True, null=True)
    delta = models.FloatField(blank=True, null=True)
    rebate = models.FloatField(blank=True, null=True)
    t = models.IntegerField(blank=True, null=True)
    r = models.FloatField(blank=True, null=True)
    q = models.FloatField(blank=True, null=True)
    v1 = models.FloatField(blank=True, null=True)
    v2 = models.FloatField(blank=True, null=True)
    expiry_003846 = models.DateField(blank=True, null=True)
    expiry_007692 = models.DateField(blank=True, null=True)
    expiry_008333 = models.DateField(blank=True, null=True)
    expiry_015384 = models.DateField(blank=True, null=True)
    expiry_016666 = models.DateField(blank=True, null=True)
    expiry_025 = models.DateField(blank=True, null=True)
    expiry_05 = models.DateField(blank=True, null=True)
    ranking = models.IntegerField(blank=True, null=True)
    class Meta:
        managed = True
        db_table = "latest_bot_update"

    def __str__(self):
        return f"{self.ticker.ticker}-{self.bot_id}"

class LatestBotData(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE,db_column="ticker", related_name="latest_bot_data_ticker", primary_key=True)
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
        db_table = "latest_bot_data"

    def __str__(self):
        return self.ticker.ticker