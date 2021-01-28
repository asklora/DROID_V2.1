# Generated by Django 3.1.4 on 2021-01-28 10:09

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Currency',
            fields=[
                ('currency_code', models.CharField(max_length=30, primary_key=True, serialize=False)),
                ('ric', models.CharField(blank=True, max_length=255, null=True)),
                ('currency_name', models.CharField(blank=True, max_length=255, null=True)),
                ('is_decimal', models.BooleanField(default=False)),
                ('is_active', models.BooleanField(default=True)),
                ('amount', models.FloatField(blank=True, null=True)),
                ('last_price', models.FloatField(blank=True, null=True)),
                ('last_date', models.DateField(blank=True, null=True)),
                ('utc_timezone_location', models.CharField(blank=True, max_length=255, null=True)),
                ('ingestion_time', models.TimeField(blank=True, null=True)),
                ('utc_offset', models.CharField(blank=True, max_length=100, null=True)),
                ('market_close_time', models.TimeField(blank=True, null=True)),
                ('market_open_time', models.TimeField(blank=True, null=True)),
                ('close_ingestion_offset', models.CharField(blank=True, max_length=100, null=True)),
                ('intraday_offset_close', models.CharField(blank=True, max_length=100, null=True)),
                ('intraday_offset_open', models.CharField(blank=True, max_length=100, null=True)),
                ('classic_schedule', models.TimeField(blank=True, null=True)),
            ],
            options={
                'db_table': 'currency',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Industry',
            fields=[
                ('industry_code', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('industry_name', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'industry',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='IndustryGroup',
            fields=[
                ('industry_group_code', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('industry_group_name', models.CharField(blank=True, max_length=100, null=True)),
                ('industry_group_img', models.FileField(blank=True, null=True, upload_to='')),
            ],
            options={
                'db_table': 'industry_group',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='IndustryWorldscope',
            fields=[
                ('wc_industry_code', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('wc_industry_name', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'industry_worldscope',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Region',
            fields=[
                ('region_id', models.CharField(max_length=30, primary_key=True, serialize=False)),
                ('region_name', models.CharField(blank=True, max_length=30, null=True)),
                ('ingestion_time', models.TimeField(blank=True, null=True)),
            ],
            options={
                'db_table': 'region',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Source',
            fields=[
                ('source_id', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('source_name', models.CharField(blank=True, max_length=100, null=True)),
            ],
            options={
                'db_table': 'source',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Universe',
            fields=[
                ('ticker', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('created', models.DateField(blank=True, null=True)),
                ('updated', models.DateField(blank=True, null=True)),
                ('last_ingestion', models.DateField(blank=True, null=True)),
                ('is_active', models.BooleanField(default=True)),
                ('quandl_symbol', models.TextField(blank=True, null=True)),
                ('ticker_name', models.TextField(blank=True, null=True)),
                ('ticker_fullname', models.TextField(blank=True, null=True)),
                ('company_description', models.TextField(blank=True, null=True)),
                ('lot_size', models.IntegerField(blank=True, null=True)),
                ('worldscope_identifier', models.CharField(blank=True, max_length=500, null=True)),
                ('icb_code', models.CharField(blank=True, max_length=500, null=True)),
                ('fiscal_year_end', models.CharField(blank=True, max_length=500, null=True)),
                ('entity_type', models.TextField(blank=True, null=True)),
                ('currency_code', models.ForeignKey(blank=True, db_column='currency_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='universe_currency_code', to='universe.currency')),
                ('industry_code', models.ForeignKey(blank=True, db_column='industry_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='universe_industry_code', to='universe.industry')),
                ('wc_industry_code', models.ForeignKey(blank=True, db_column='wc_industry_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='universe_wc_industry_code', to='universe.industryworldscope')),
            ],
            options={
                'db_table': 'universe',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Vix',
            fields=[
                ('vix_id', models.CharField(max_length=100, primary_key=True, serialize=False)),
            ],
            options={
                'db_table': 'vix',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='UniverseExcluded',
            fields=[
                ('ticker', models.OneToOneField(db_column='ticker', on_delete=django.db.models.deletion.CASCADE, primary_key=True, related_name='universe_excluded_ticker', serialize=False, to='universe.universe')),
                ('exclude_dss', models.BooleanField(default=False)),
                ('exclude_dsws', models.BooleanField(default=False)),
            ],
            options={
                'db_table': 'universe_excluded',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='UniverseRating',
            fields=[
                ('ticker', models.OneToOneField(db_column='ticker', on_delete=django.db.models.deletion.CASCADE, primary_key=True, related_name='ticker_rating_ticker', serialize=False, to='universe.universe')),
                ('fundamentals_quality', models.FloatField(blank=True, null=True)),
                ('fundamentals_value', models.FloatField(blank=True, null=True)),
                ('dlp_1m', models.FloatField(blank=True, null=True)),
                ('dlp_3m', models.FloatField(blank=True, null=True)),
                ('wts_rating', models.FloatField(blank=True, null=True)),
                ('wts_rating2', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'universe_rating',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='UniverseConsolidated',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('origin_ticker', models.CharField(max_length=10)),
                ('is_active', models.BooleanField(default=False)),
                ('created', models.DateField(blank=True, null=True)),
                ('updated', models.DateField(blank=True, null=True)),
                ('isin', models.CharField(blank=True, max_length=500, null=True)),
                ('use_isin', models.BooleanField(default=False)),
                ('cusip', models.CharField(blank=True, max_length=500, null=True)),
                ('use_cusip', models.BooleanField(default=False)),
                ('sedol', models.CharField(blank=True, max_length=500, null=True)),
                ('use_sedol', models.BooleanField(default=False)),
                ('use_manual', models.BooleanField(default=False)),
                ('consolidated_ticker', models.CharField(max_length=10)),
                ('source_id', models.ForeignKey(blank=True, db_column='source_id', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='universe_source_id', to='universe.source')),
            ],
            options={
                'db_table': 'universe_consolidated',
                'managed': True,
            },
        ),
        migrations.AddField(
            model_name='industry',
            name='industry_group_code',
            field=models.ForeignKey(blank=True, db_column='industry_group_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='industry_industry_group_code', to='universe.industrygroup'),
        ),
        migrations.CreateModel(
            name='CurrencyCalendars',
            fields=[
                ('uid', models.TextField(primary_key=True, serialize=False)),
                ('non_working_day', models.DateField(blank=True, null=True)),
                ('description', models.TextField(blank=True, null=True)),
                ('currency_code', models.ForeignKey(blank=True, db_column='currency_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='currency_calendar_currency_code', to='universe.currency')),
            ],
            options={
                'db_table': 'currency_calendar',
                'managed': True,
            },
        ),
        migrations.AddField(
            model_name='currency',
            name='region_id',
            field=models.ForeignKey(db_column='region_id', on_delete=django.db.models.deletion.CASCADE, related_name='currency_region_id', to='universe.region'),
        ),
        migrations.AddField(
            model_name='currency',
            name='vix_id',
            field=models.ForeignKey(db_column='vix_id', on_delete=django.db.models.deletion.CASCADE, related_name='currency_vix_id', to='universe.vix'),
        ),
    ]
