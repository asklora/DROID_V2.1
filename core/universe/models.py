from django.db import models

class Region(models.Model):
    region_id = models.CharField(primary_key=True, max_length=30)
    region_name = models.CharField(blank=True, null=True, max_length=30)
    ingestion_time = models.TimeField(blank=True, null=True)

    def __str__(self):
        return self.region_name

    class Meta:
        managed = True
        db_table = 'region'


class Vix(models.Model):
    vix_id = models.CharField(max_length=100, primary_key=True)

    class Meta:
        managed = True
        db_table = 'vix'


class Currency(models.Model):
    currency_code = models.CharField(primary_key=True, max_length=30)
    region_id = models.ForeignKey(Region, on_delete=models.CASCADE,db_column='region_id', related_name='currency_region_id')
    vix_id = models.ForeignKey(Vix, on_delete=models.CASCADE, db_column='vix_id', related_name='currency_vix_id')

    ric = models.CharField(blank=True, null=True, max_length=255)
    currency_name = models.CharField(blank=True, null=True, max_length=255)
    is_decimal = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    amount = models.FloatField(blank=True, null=True)
    last_price = models.FloatField(blank=True, null=True)
    last_date = models.DateField(blank=True, null=True)

    utc_timezone_location = models.CharField(blank=True, null=True, max_length=255)
    ingestion_time = models.TimeField(null=True, blank=True)
    utc_offset = models.CharField(blank=True, null=True, max_length=100)
    market_close_time = models.TimeField(null=True, blank=True)
    market_open_time = models.TimeField(null=True, blank=True)
    close_ingestion_offset = models.CharField(blank=True, null=True, max_length=100)
    intraday_offset_close = models.CharField(blank=True, null=True, max_length=100)
    intraday_offset_open = models.CharField(blank=True, null=True, max_length=100)
    classic_schedule = models.TimeField(blank=True, null=True)

    def __str__(self):
        return self.currency_code

    class Meta:
        managed = True
        db_table = 'currency'


class CurrencyCalendars(models.Model):
    uid = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column='currency_code',related_name='currency_calendar_currency_code', blank=True, null=True)
    non_working_day = models.DateField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'currency_calendar'


class Country(models.Model):
    country_code = models.TextField(primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column='currency_code',related_name='country_currency_code', blank=True, null=True)
    country_name = models.TextField(blank=True, null=True)
    ds_country_code = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'country'


class CountryCalendars(models.Model):
    uid = models.TextField(primary_key=True)
    country_code = models.ForeignKey(Country, on_delete=models.CASCADE, db_column='country_code',related_name='country_calendar_country_code', blank=True, null=True)
    non_working_day = models.DateField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'country_calendar'


class IndustryGroup(models.Model):
    industry_group_code = models.CharField(max_length=100, primary_key=True)
    industry_group_name = models.CharField(max_length=100, blank=True, null=True)
    industry_group_img = models.FileField(blank=True, null=True)

    def __str__(self):
        return self.industry_group_name

    class Meta:
        managed = True
        db_table = 'industry_group'


class Industry(models.Model):
    industry_code = models.CharField(max_length=100, primary_key=True)
    industry_name = models.CharField(max_length=100, blank=True, null=True)
    industry_group_code = models.ForeignKey(IndustryGroup, on_delete=models.CASCADE, db_column='industry_group_code',related_name='industry_industry_group_code', blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'industry'

    def __str__(self):
        return self.industry_name


class IndustryWorldscope(models.Model):
    wc_industry_code = models.CharField(max_length=100, primary_key=True)
    wc_industry_name = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'industry_worldscope'


class CountryDial(models.Model):
    country_name = models.CharField(max_length=500, blank=True, null=True)
    country_name_english = models.CharField(
        max_length=500, blank=True, null=True)
    country_code_iso2 = models.CharField(max_length=500, blank=True, null=True)
    country_code_iso3 = models.CharField(max_length=500, blank=True, null=True)
    country_dial_code = models.CharField(max_length=500, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'country_dial'


class Source(models.Model):
    source_id = models.CharField(max_length=100, primary_key=True)
    source_name = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'source'

    def __str__(self):
        return self.source_name


class UniverseConsolidated(models.Model):
    id = models.AutoField(primary_key=True)
    source_id = models.ForeignKey(Source, on_delete=models.CASCADE, db_column='source_id', related_name='universe_source_id', blank=True, null=True)
    origin_ticker = models.CharField(max_length=10, blank=False, null=False)
    is_active = models.BooleanField(default=True)
    created = models.DateField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)
    
    isin = models.CharField(max_length=500, blank=True, null=True)
    ticker_fullname = models.CharField(max_length=500, blank=True, null=True)
    use_isin = models.BooleanField(default=False)

    cusip = models.CharField(max_length=500, blank=True, null=True)
    use_cusip = models.BooleanField(default=False)

    sedol = models.CharField(max_length=500, blank=True, null=True)
    use_sedol = models.BooleanField(default=False)

    use_manual = models.BooleanField(default=False)

    consolidated_ticker = models.CharField(max_length=10)
    # manager = UniverseConsolidatedManager()
    # objects = models.Manager()

    class Meta:
        managed = True
        db_table = 'universe_consolidated'

    def __str__(self):
        return self.consolidated_ticker


class Universe(models.Model):
    # objects = models.Manager()
    # manager = UniverseManager()
    ticker = models.CharField(max_length=255,primary_key=True)
    currency_code = models.ForeignKey(Currency, on_delete=models.CASCADE, db_column='currency_code', related_name='universe_currency_code', blank=True, null=True)
    #country_code = models.ForeignKey(Country, on_delete=models.CASCADE, db_column='country_code', related_name='universe_country_code', blank=True, null=True)
    industry_code = models.ForeignKey(Industry, on_delete=models.CASCADE, db_column='industry_code', related_name='universe_industry_code', blank=True, null=True)
    wc_industry_code = models.ForeignKey(IndustryWorldscope, on_delete=models.CASCADE,db_column='wc_industry_code', related_name='universe_wc_industry_code', blank=True, null=True)

    created = models.DateField(blank=True, null=True)
    updated = models.DateField(blank=True, null=True)
    last_ingestion = models.DateField(blank=True, null=True)

    is_active = models.BooleanField(default=True)
    quandl_symbol = models.TextField(blank=True, null=True)
    ticker_name = models.TextField(blank=True, null=True)
    ticker_fullname = models.TextField(blank=True, null=True)
    company_description = models.TextField(blank=True, null=True)
    lot_size = models.IntegerField(blank=True, null=True)
    worldscope_identifier = models.CharField(max_length=500, blank=True, null=True)
    icb_code = models.CharField(max_length=500, blank=True, null=True)
    fiscal_year_end = models.CharField(max_length=500, blank=True, null=True)
    entity_type = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.ticker

    class Meta:
        managed = True
        db_table = 'universe'


class UniverseRating(models.Model):
    ticker = models.OneToOneField(Universe, on_delete=models.CASCADE,
                                  db_column='ticker', related_name='ticker_rating_ticker', primary_key=True)
    fundamentals_quality = models.FloatField(blank=True, null=True)
    fundamentals_value = models.FloatField(blank=True, null=True)
    dlp_1m = models.FloatField(blank=True, null=True)
    dlp_3m = models.FloatField(blank=True, null=True)
    wts_rating = models.FloatField(blank=True, null=True)
    wts_rating2 = models.FloatField(blank=True, null=True)

    def __str__(self):
        return self.ticker.ticker

    class Meta:
        db_table = 'universe_rating'
        managed = True


class UniverseExcluded(models.Model):
    ticker = models.OneToOneField(Universe, primary_key=True, on_delete=models.CASCADE,
                                  db_column='ticker', related_name='universe_excluded_ticker')
    exclude_dss = models.BooleanField(default=False)
    exclude_dsws = models.BooleanField(default=False)

    class Meta:
        managed = True
        db_table = 'universe_excluded'

    def __str__(self):
        return self.ticker.ticker
