# Generated by Django 3.1.4 on 2021-03-25 03:56

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0015_auto_20210316_0813'),
        ('bot', '0013_latestvol'),
    ]

    operations = [
        migrations.CreateModel(
            name='LatestBotData',
            fields=[
                ('ticker', models.OneToOneField(db_column='ticker', on_delete=django.db.models.deletion.CASCADE, primary_key=True, related_name='latest_bot_data_ticker', serialize=False, to='universe.universe')),
                ('trading_day', models.DateField(blank=True, null=True)),
                ('c2c_vol_0_21', models.FloatField(blank=True, null=True)),
                ('c2c_vol_21_42', models.FloatField(blank=True, null=True)),
                ('c2c_vol_42_63', models.FloatField(blank=True, null=True)),
                ('c2c_vol_63_126', models.FloatField(blank=True, null=True)),
                ('c2c_vol_126_252', models.FloatField(blank=True, null=True)),
                ('c2c_vol_252_504', models.FloatField(blank=True, null=True)),
                ('kurt_0_504', models.FloatField(blank=True, null=True)),
                ('rs_vol_0_21', models.FloatField(blank=True, null=True)),
                ('rs_vol_21_42', models.FloatField(blank=True, null=True)),
                ('rs_vol_42_63', models.FloatField(blank=True, null=True)),
                ('rs_vol_63_126', models.FloatField(blank=True, null=True)),
                ('rs_vol_126_252', models.FloatField(blank=True, null=True)),
                ('rs_vol_252_504', models.FloatField(blank=True, null=True)),
                ('total_returns_0_1', models.FloatField(blank=True, null=True)),
                ('total_returns_0_21', models.FloatField(blank=True, null=True)),
                ('total_returns_0_63', models.FloatField(blank=True, null=True)),
                ('total_returns_21_126', models.FloatField(blank=True, null=True)),
                ('total_returns_21_231', models.FloatField(blank=True, null=True)),
                ('vix_value', models.FloatField(blank=True, null=True)),
                ('atm_volatility_spot', models.FloatField(blank=True, null=True)),
                ('atm_volatility_one_year', models.FloatField(blank=True, null=True)),
                ('atm_volatility_infinity', models.FloatField(blank=True, null=True)),
                ('slope', models.FloatField(blank=True, null=True)),
                ('deriv', models.FloatField(blank=True, null=True)),
                ('slope_inf', models.FloatField(blank=True, null=True)),
                ('deriv_inf', models.FloatField(blank=True, null=True)),
                ('atm_volatility_spot_x', models.FloatField(blank=True, null=True)),
                ('atm_volatility_one_year_x', models.FloatField(blank=True, null=True)),
                ('atm_volatility_infinity_x', models.FloatField(blank=True, null=True)),
                ('total_returns_0_63_x', models.FloatField(blank=True, null=True)),
                ('total_returns_21_126_x', models.FloatField(blank=True, null=True)),
                ('total_returns_0_21_x', models.FloatField(blank=True, null=True)),
                ('total_returns_21_231_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_0_21_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_21_42_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_42_63_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_63_126_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_126_252_x', models.FloatField(blank=True, null=True)),
                ('c2c_vol_252_504_x', models.FloatField(blank=True, null=True)),
            ],
            options={
                'db_table': 'latest_bot_data',
                'managed': True,
            },
        ),
    ]
