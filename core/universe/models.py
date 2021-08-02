from core.djangomodule.models import BaseTimeStampModel
from django.db import models
from .manager import ConsolidatedManager, UniverseManager
from core.djangomodule.general import generate_id
from django.db import IntegrityError
from django.utils import timezone


class Region(models.Model):
    region_id = models.CharField(primary_key=True, max_length=30)
    region_name = models.CharField(blank=True, null=True, max_length=30)
    ingestion_time = models.TimeField(blank=True, null=True)

    def __str__(self):
        return self.region_id

    class Meta:
        managed = True
        db_table = "region"


class Vix(models.Model):
    vix_id = models.CharField(max_length=100, primary_key=True)

    class Meta:
        managed = True
        db_table = "vix"

    def __str__(self):
        return self.vix_id


class Currency(models.Model):
    currency_code = models.CharField(primary_key=True, max_length=30)
    region_id = models.ForeignKey(
        Region, on_delete=models.CASCADE, db_column="region_id", related_name="currency_region_id")
    vix_id = models.ForeignKey(
        Vix, on_delete=models.CASCADE, db_column="vix_id", related_name="currency_vix_id")

    ric = models.CharField(blank=True, null=True, max_length=255)
    currency_name = models.CharField(blank=True, null=True, max_length=255)
    is_decimal = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    amount = models.FloatField(blank=True, null=True)
    last_price = models.FloatField(blank=True, null=True)
    last_date = models.DateField(blank=True, null=True)
    utc_timezone_location = models.CharField(
        blank=True, null=True, max_length=255)
    ingestion_time = models.TimeField(null=True, blank=True)
    utc_offset = models.CharField(blank=True, null=True, max_length=100)
    market_close_time = models.TimeField(null=True, blank=True)
    market_open_time = models.TimeField(null=True, blank=True)
    close_ingestion_offset = models.CharField(
        blank=True, null=True, max_length=100)
    intraday_offset_close = models.CharField(
        blank=True, null=True, max_length=100)
    intraday_offset_open = models.CharField(
        blank=True, null=True, max_length=100)
    backtest_schedule = models.TimeField(blank=True, null=True)
    top_stock_schedule = models.TimeField(blank=True, null=True)
    hedge_schedule = models.TimeField(blank=True, null=True)
    index_ticker = models.TextField(blank=True, null=True)
    index_price = models.FloatField(blank=True, null=True)

    def __str__(self):
        return self.currency_code

    class Meta:
        managed = True
        db_table = "currency"


class CurrencyCalendars(models.Model):
    uid = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code",
                                      related_name="currency_calendar_currency_code", blank=True, null=True)
    non_working_day = models.DateField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "currency_calendar"

    def __str__(self):
        return self.uid


class Country(models.Model):
    country_code = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code",
                                      related_name="country_currency_code", blank=True, null=True)
    country_name = models.TextField(blank=True, null=True)
    ds_country_code = models.TextField(blank=True, null=True)
    is_open = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = "country"

    def __str__(self):
        return self.country_code

# class CountryCalendars(models.Model):
#     uid = models.TextField(primary_key=True)
#     country_code = models.ForeignKey(Country, on_delete=models.CASCADE, db_column="country_code",related_name="country_calendar_country_code", blank=True, null=True)
#     non_working_day = models.DateField(blank=True, null=True)
#     description = models.TextField(blank=True, null=True)

#     class Meta:
#         managed = False
#         db_table = "country_calendar"


class IndustryGroup(models.Model):
    industry_group_code = models.CharField(max_length=100, primary_key=True)
    industry_group_name = models.CharField(
        max_length=100, blank=True, null=True)
    industry_group_img = models.FileField(blank=True, null=True)

    def __str__(self):
        return self.industry_group_code

    class Meta:
        managed = True
        db_table = "industry_group"


class Industry(models.Model):
    industry_code = models.CharField(max_length=100, primary_key=True)
    industry_name = models.CharField(max_length=100, blank=True, null=True)
    industry_group_code = models.ForeignKey(IndustryGroup, on_delete=models.CASCADE,
                                            db_column="industry_group_code", related_name="industry_industry_group_code", blank=True, null=True)

    class Meta:
        managed = True
        db_table = "industry"

    def __str__(self):
        return self.industry_code


class IndustryWorldscope(models.Model):
    wc_industry_code = models.CharField(max_length=100, primary_key=True)
    wc_industry_name = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = "industry_worldscope"

    def __str__(self):
        return self.wc_industry_code


class Source(models.Model):
    source_id = models.CharField(max_length=100, primary_key=True)
    source_name = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = "source"

    def __str__(self):
        return self.source_id


class UniverseConsolidated(models.Model):
    uid = models.CharField(primary_key=True, max_length=20, editable=False)
    source_id = models.ForeignKey(Source, on_delete=models.CASCADE, db_column="source_id",
                                  related_name="universe_source_id", blank=True, null=True)
    origin_ticker = models.CharField(max_length=50, blank=False, null=False)
    is_active = models.BooleanField(default=True)
    created = models.DateField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)
    isin = models.CharField(max_length=500, blank=True, null=True)
    use_isin = models.BooleanField(default=False)
    cusip = models.CharField(max_length=500, blank=True, null=True)
    use_cusip = models.BooleanField(default=False)
    sedol = models.CharField(max_length=500, blank=True, null=True)
    use_sedol = models.BooleanField(default=False)
    use_manual = models.BooleanField(default=False)
    permid = models.CharField(max_length=500, blank=True, null=True)
    consolidated_ticker = models.CharField(
        max_length=50, blank=True, null=True)
    ingestion_manager = ConsolidatedManager()
    has_data = models.BooleanField(default=False)
    objects = models.Manager()

    def save(self, *args, **kwargs):
        if not self.uid:
            self.uid = generate_id(8)
            if not self.created:
                self.created = timezone.now()
                self.updated = timezone.now()
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(UniverseConsolidated, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.uid = generate_id(8)
            else:
                success = True
                self.updated = timezone.now()

    class Meta:
        managed = True
        db_table = "universe_consolidated"

    def __str__(self):
        if self.consolidated_ticker:
            return self.consolidated_ticker
        else:
            return self.origin_ticker


class Universe(models.Model):
    objects = models.Manager()
    manager = UniverseManager()
    ticker = models.CharField(max_length=255, primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column="currency_code",
                                      related_name="universe_currency_code", blank=True, null=True)
    #country_code = models.ForeignKey(Country, on_delete=models.CASCADE, db_column="country_code", related_name="universe_country_code", blank=True, null=True)
    industry_code = models.ForeignKey(Industry, on_delete=models.CASCADE, db_column="industry_code",
                                      related_name="universe_industry_code", blank=True, null=True)
    wc_industry_code = models.ForeignKey(IndustryWorldscope, on_delete=models.CASCADE,
                                         db_column="wc_industry_code", related_name="universe_wc_industry_code", blank=True, null=True)

    created = models.DateField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)
    last_ingestion = models.DateField(blank=True, null=True)

    is_active = models.BooleanField(default=True)
    quandl_symbol = models.TextField(blank=True, null=True)
    ticker_name = models.TextField(blank=True, null=True)
    ticker_fullname = models.TextField(blank=True, null=True)
    company_description = models.TextField(blank=True, null=True)
    lot_size = models.IntegerField(blank=True, null=True)
    worldscope_identifier = models.CharField(
        max_length=500, blank=True, null=True)
    icb_code = models.CharField(max_length=500, blank=True, null=True)
    fiscal_year_end = models.CharField(max_length=500, blank=True, null=True)
    entity_type = models.TextField(blank=True, null=True)
    ticker_symbol = models.TextField(blank=True, null=True)
    mic = models.TextField(blank=True, null=True)
    revenue_per_share=models.FloatField(null=True,blank=True)
    market_cap=models.FloatField(null=True,blank=True)
    pe_ratio=models.FloatField(null=True,blank=True)
    pe_forecast=models.FloatField(null=True,blank=True)
    pb=models.FloatField(null=True,blank=True)
    ev=models.FloatField(null=True,blank=True)
    ebitda=models.FloatField(null=True,blank=True)
    wk52_high =models.FloatField(null=True,blank=True)
    wk52_low =models.FloatField(null=True,blank=True)
    free_cash_flow =models.FloatField(null=True,blank=True)
    Error = models.TextField(null=True,blank=True)
    schi_name = models.TextField(null=True,blank=True)
    tchi_name = models.TextField(null=True,blank=True)
    def __str__(self):
        return self.ticker

    class Meta:
        managed = True
        db_table = "universe"
        indexes = [models.Index(fields=['ticker_symbol','currency_code','mic'])]
        
#test

class UniverseRating(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="ticker_rating_ticker", primary_key=True)
    fundamentals_quality = models.FloatField(blank=True, null=True)
    fundamentals_value = models.FloatField(blank=True, null=True)
    dlp_1m = models.FloatField(blank=True, null=True)
    dlp_3m = models.FloatField(blank=True, null=True)
    wts_rating = models.FloatField(blank=True, null=True)
    wts_rating2 = models.FloatField(blank=True, null=True)
    classic_vol = models.FloatField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)

    def __str__(self):
        return str(self.ticker.ticker)

    class Meta:
        db_table = "universe_rating"
        managed = True

class UniverseRatingHistory(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,db_column="ticker", related_name="universe_rating_history_ticker")
    trading_day = models.DateField(blank=True, null=True)
    ai_score = models.FloatField(blank=True, null=True)
    ai_score2 = models.FloatField(blank=True, null=True)
    fundamentals_quality = models.FloatField(blank=True, null=True)
    fundamentals_value = models.FloatField(blank=True, null=True)
    technical = models.FloatField(blank=True, null=True)
    momentum = models.FloatField(blank=True, null=True)
    esg = models.FloatField(blank=True, null=True)
    dlp_1m = models.FloatField(blank=True, null=True)
    dlp_3m = models.FloatField(blank=True, null=True)
    wts_rating = models.FloatField(blank=True, null=True)
    wts_rating2 = models.FloatField(blank=True, null=True)
    classic_vol = models.FloatField(blank=True, null=True)

    def __str__(self):
        return str(self.ticker.ticker)

    class Meta:
        db_table = "universe_rating_history"
        managed = True

class UniverseRatingDetailHistory(models.Model):
    uid = models.TextField(primary_key=True)
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE,db_column="ticker", related_name="universe_rating_detail_history_ticker")
    trading_day = models.DateField(blank=True, null=True)
    earnings_yield_minmax_currency_code = models.FloatField(blank=True, null=True)
    earnings_yield_minmax_industry = models.FloatField(blank=True, null=True)
    book_to_price_minmax_currency_code = models.FloatField(blank=True, null=True)
    book_to_price_minmax_industry = models.FloatField(blank=True, null=True)
    ebitda_to_ev_minmax_currency_code = models.FloatField(blank=True, null=True)
    ebitda_to_ev_minmax_industry = models.FloatField(blank=True, null=True)
    sales_to_price_minmax_currency_code = models.FloatField(blank=True, null=True)
    sales_to_price_minmax_industry = models.FloatField(blank=True, null=True)
    roic_minmax_currency_code = models.FloatField(blank=True, null=True)
    roic_minmax_industry = models.FloatField(blank=True, null=True)
    roe_minmax_currency_code = models.FloatField(blank=True, null=True)
    roe_minmax_industry = models.FloatField(blank=True, null=True)
    cf_to_price_minmax_currency_code = models.FloatField(blank=True, null=True)
    cf_to_price_minmax_industry = models.FloatField(blank=True, null=True)
    eps_growth_minmax_currency_code = models.FloatField(blank=True, null=True)
    eps_growth_minmax_industry = models.FloatField(blank=True, null=True)
    fwd_bps_minmax_currency_code = models.FloatField(blank=True, null=True)
    fwd_bps_minmax_industry = models.FloatField(blank=True, null=True)
    fwd_ebitda_to_ev_minmax_currency_code = models.FloatField(blank=True, null=True)
    fwd_ebitda_to_ev_minmax_industry = models.FloatField(blank=True, null=True)
    fwd_ey_minmax_currency_code = models.FloatField(blank=True, null=True)
    fwd_ey_minmax_industry = models.FloatField(blank=True, null=True)
    fwd_sales_to_price_minmax_currency_code = models.FloatField(blank=True, null=True)
    fwd_sales_to_price_minmax_industry = models.FloatField(blank=True, null=True)
    fwd_roic_minmax_currency_code = models.FloatField(blank=True, null=True)
    fwd_roic_minmax_industry = models.FloatField(blank=True, null=True)
    earnings_pred_minmax_currency_code = models.FloatField(blank=True, null=True)
    earnings_pred_minmax_industry = models.FloatField(blank=True, null=True)
    environment_minmax_currency_code = models.FloatField(blank=True, null=True)
    social_minmax_currency_code = models.FloatField(blank=True, null=True)
    goverment_minmax_currency_code = models.FloatField(blank=True, null=True)
    environment_minmax_industry = models.FloatField(blank=True, null=True)
    social_minmax_industry = models.FloatField(blank=True, null=True)
    goverment_minmax_industry = models.FloatField(blank=True, null=True)
    tri_quantile = models.FloatField(blank=True, null=True)

    def __str__(self):
        return str(self.ticker.ticker)

    class Meta:
        db_table = "universe_rating_detail_history"
        managed = True


class UniverseExcluded(models.Model):
    ticker = models.OneToOneField(Universe, primary_key=True, on_delete=models.CASCADE,
                                  db_column="ticker", related_name="universe_excluded_ticker")
    exclude_dss = models.BooleanField(default=False)
    exclude_dsws = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = "universe_excluded"

    def __str__(self):
        return str(self.ticker.ticker)


class SpecialCases(models.Model):
    ticker = models.ForeignKey(Universe, on_delete=models.CASCADE, db_column="ticker", related_name="special_cases_ticker")
    cases = models.IntegerField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    last_updated = models.DateField(blank=True, null=True)
    date_source = models.DateField(blank=True, null=True)
    date_replace = models.DateField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "special_cases"

    def __str__(self):
        return str(self.ticker.ticker)
    
class ExchangeMarket(BaseTimeStampModel):
    mic=models.CharField(max_length=20, primary_key=True)
    fin_id= models.CharField(blank=True, null=True,max_length=255)
    exchange= models.CharField(blank=True, null=True,max_length=255)
    market= models.CharField(blank=True, null=True,max_length=255)
    products= models.CharField(blank=True, null=True,max_length=255)
    asset_type= models.CharField(blank=True, null=True,max_length=255)
    group= models.CharField(blank=True, null=True,max_length=100)
    currency_code = models.ForeignKey(Currency,null=True,blank=True,related_name="currency_exchange", on_delete=models.CASCADE,db_column="currency_code")

    class Meta:
        managed = True
        db_table = "universe_exchange_market"

    def __str__(self):
        return self.mic

