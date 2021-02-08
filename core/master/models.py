from django.db import models
from core.universe.models import Universe, Currency, Vix
from psqlextra.manager import PostgresManager
# from .manager import MasterOhlcvtrManager, DataQuandlManager, DssDataManager, DswsDataManager, DataDividendManager


class DataDss(models.Model):
    dss_id = models.CharField(max_length=30, primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_dss_ticker")
    trading_day = models.DateField()
    open = models.FloatField(null=True, blank=True)
    high = models.FloatField(null=True, blank=True)
    low = models.FloatField(null=True, blank=True)
    close = models.FloatField(null=True, blank=True)
    volume = models.FloatField(null=True, blank=True)
    # manager = DssDataManager()
    objects = models.Manager()

    class Meta:
        managed = True
        db_table = "data_dss"

    def __str__(self):
        return self.ticker


class DataDsws(models.Model):
    dsws_id = models.CharField(max_length=30, primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_dsws_ticker")
    trading_day = models.DateField()
    total_return_index = models.FloatField(null=True, blank=True)
    # manager = DswsDataManager()
    objects = models.Manager()

    class Meta:
        managed = True
        db_table = "data_dsws"

    def __str__(self):
        return self.ticker


class ReportDatapoint(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE,
                                  db_column="ticker", related_name="dss_datapoint_ticker", primary_key=True)
    datapoint = models.IntegerField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)
    reingested = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = "report_datapoint"

    def __str__(self):
        return self.ticker


class MasterMultiple(models.Model):  # Master Daily Change to Master Multiply
    uid = models.CharField(max_length=30, primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,db_column="ticker", related_name="master_multiple_ticker")
    trading_day = models.DateField(blank=True, null=True)
    open_multiple = models.FloatField(blank=True, null=True)
    high_multiple = models.FloatField(blank=True, null=True)
    low_multiple = models.FloatField(blank=True, null=True)
    close_multiple = models.FloatField(blank=True, null=True)
    volume_adj = models.FloatField(blank=True, null=True)
    turn_over = models.FloatField(blank=True, null=True)
    tri = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "master_multiple"

    def __str__(self):
        return self.ticker


class MasterOhlcvtr(models.Model):  # OHLCVTR DATA Change to master_ohlcvtr
    uid = models.CharField(max_length=30, primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="master_ohlcvtr_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                      db_column="currency_code", related_name="master_ohlcvtr_currency_code")
    trading_day = models.DateField(blank=True, null=True)
    open = models.FloatField(blank=True, null=True)
    high = models.FloatField(blank=True, null=True)
    low = models.FloatField(blank=True, null=True)
    close = models.FloatField(blank=True, null=True)
    volume = models.FloatField(blank=True, null=True)
    total_return_index = models.FloatField(blank=True, null=True)
    day_status = models.TextField(blank=True, null=True)
    datapoint = models.IntegerField(blank=True, null=True)
    datapoint_per_day = models.IntegerField(blank=True, null=True)
    # manager = MasterOhlcvtrManager()
    objects = models.Manager()

    class Meta:
        managed = True
        db_table = "master_ohlcvtr"

    def __str__(self):
        return self.ticker


class MasterTac(models.Model):  # Master TAC Change to master_tac
    uid = models.CharField(max_length=30, primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="master_tac_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code", related_name="master_tac_currency_code")
    trading_day = models.DateField(blank=True, null=True)
    volume = models.FloatField(blank=True, null=True)
    total_return_index = models.FloatField(blank=True, null=True)
    tri_adj_open = models.FloatField(blank=True, null=True)
    tri_adj_high = models.FloatField(blank=True, null=True)
    tri_adj_low = models.FloatField(blank=True, null=True)
    tri_adj_close = models.FloatField(blank=True, null=True)
    rsi = models.FloatField(blank=True, null=True)
    fast_k = models.FloatField(blank=True, null=True)
    fast_d = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "master_tac"

    def __str__(self):
        return self.ticker


class DataDividend(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_dividend_ticker")
    ex_dividend_date = models.DateField(blank=True, null=True)
    amount = models.FloatField(blank=True, null=True)
    # manager = DataDividendManager()
    objects = models.Manager()

    class Meta:
        managed = True
        db_table = "data_dividend"

    def __str__(self):
        return self.ticker


class DataInterest(models.Model):
    ticker_interest = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                      db_column="currency_code", related_name="data_interest_currency_code")
    rate = models.FloatField(blank=True, null=True)
    raw_data = models.FloatField(blank=True, null=True)
    days_to_maturity = models.IntegerField(blank=True, null=True)
    ingestion_field = models.TextField(blank=True, null=True)
    maturity = models.DateField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_interest"

    def __str__(self):
        return self.ticker_interest


class DataDividendDailyRates(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_dividend_daily_rates_ticker")
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                      db_column="currency_code", related_name="data_dividend_daily_rates_currency_code")
    q = models.FloatField(blank=True, null=True)
    t = models.IntegerField(blank=True, null=True)
    spot_date = models.DateField(blank=True, null=True)
    expiry_date = models.DateField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_dividend_daily_rates"

    def __str__(self):
        return self.ticker


class DataInterestDailyRates(models.Model):
    uid = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                      db_column="currency_code", related_name="data_interest_daily_rates_currency_code")
    r = models.FloatField(blank=True, null=True)
    t = models.IntegerField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_interest_daily_rates"

    def __str__(self):
        return self.uid


class DataSplit(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE,
                                  db_column="ticker", related_name="data_split_ticker", primary_key=True)
    data_type = models.TextField(null=True, blank=True)
    intraday_date = models.DateField(blank=True, null=True)
    capital_change = models.FloatField(blank=True, null=True)
    price = models.FloatField(blank=True, null=True)
    percent_change = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_split"

    def __str__(self):
        return self.ticker


class DataFundamentalScore(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE, db_column="ticker",
                                  related_name="data_fundamental_score_ticker", primary_key=True)
    eps = models.FloatField(blank=True, null=True)
    bps = models.FloatField(blank=True, null=True)
    ev = models.FloatField(blank=True, null=True)
    ttm_rev = models.FloatField(blank=True, null=True)
    mkt_cap = models.FloatField(blank=True, null=True)
    ttm_ebitda = models.FloatField(blank=True, null=True)
    ttm_capex = models.FloatField(blank=True, null=True)
    net_debt = models.FloatField(blank=True, null=True)
    roe = models.FloatField(blank=True, null=True)
    cfps = models.FloatField(blank=True, null=True)
    peg = models.FloatField(blank=True, null=True)
    bps1fd12 = models.FloatField(blank=True, null=True)
    ebd1fd12 = models.FloatField(blank=True, null=True)
    evt1fd12 = models.FloatField(blank=True, null=True)
    eps1fd12 = models.FloatField(blank=True, null=True)
    sal1fd12 = models.FloatField(blank=True, null=True)
    cap1fd12 = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_fundamental_score"

    def __str__(self):
        return self.ticker


class DataQuandl(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_quandl_ticker")
    trading_day = models.DateField(blank=True, null=True)
    stockpx = models.FloatField(blank=True, null=True)
    iv30 = models.FloatField(blank=True, null=True)
    iv60 = models.FloatField(blank=True, null=True)
    iv90 = models.FloatField(blank=True, null=True)
    m1atmiv = models.FloatField(blank=True, null=True)
    m1dtex = models.FloatField(blank=True, null=True)
    m2atmiv = models.FloatField(blank=True, null=True)
    m2dtex = models.FloatField(blank=True, null=True)
    m3atmiv = models.FloatField(blank=True, null=True)
    m3dtex = models.FloatField(blank=True, null=True)
    m4atmiv = models.FloatField(blank=True, null=True)
    m4dtex = models.FloatField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)
    # manager = DataQuandlManager()
    objects = models.Manager()

    class Meta:
        managed = True
        db_table = "data_quandl"

    def __str__(self):
        return f"{self.ticker}-{self.trading_day}"


class DataVolSurface(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_vol_surface_ticker")
    trading_day = models.DateField(blank=True, null=True)
    stock_price = models.FloatField(blank=True, null=True)
    atm_volatility_spot = models.FloatField(blank=True, null=True)
    atm_volatility_one_year = models.FloatField(blank=True, null=True)
    atm_volatility_infinity = models.FloatField(blank=True, null=True)
    alpha = models.IntegerField(blank=True, null=True)
    slope = models.FloatField(blank=True, null=True)
    deriv = models.FloatField(blank=True, null=True)
    slope_inf = models.FloatField(blank=True, null=True)
    deriv_inf = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_vol_surface"

    def __str__(self):
        return f"{self.ticker}-{self.trading_day}"


class DataVolSurfaceInferred(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,
                               db_column="ticker", related_name="data_vol_surface_inferred_ticker")
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
        return f"{self.ticker}-{self.trading_day}"


class DataVix(models.Model):
    uid = models.TextField(primary_key=True)
    vix_id = models.ForeignKey(Vix, on_delete=models.CASCADE, db_column="vix_id", related_name="data_vix_vix_id")
    trading_day = models.DateField(blank=True, null=True)
    vix_value = models.FloatField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "data_vix"