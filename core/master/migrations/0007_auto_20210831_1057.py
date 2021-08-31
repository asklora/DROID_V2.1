# Generated by Django 3.2.5 on 2021-08-31 10:57

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0004_auto_20210819_0640'),
        ('master', '0006_auto_20210826_0304'),
    ]

    operations = [
        migrations.AddField(
            model_name='ibes',
            name='sal1fd12',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='ibesmonthly',
            name='sal1fd12',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macro',
            name='cboevix',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macro',
            name='vhsivol',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macro',
            name='vkospix',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macro',
            name='vstoxxi',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macromonthly',
            name='cboevix',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macromonthly',
            name='vhsivol',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macromonthly',
            name='vkospix',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='macromonthly',
            name='vstoxxi',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='worldscopesummary',
            name='fn_3040',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.CreateModel(
            name='DataFundamentalScoreHistory',
            fields=[
                ('uid', models.TextField(primary_key=True, serialize=False)),
                ('trading_day', models.DateField(blank=True, null=True)),
                ('eps', models.FloatField(blank=True, null=True)),
                ('bps', models.FloatField(blank=True, null=True)),
                ('ev', models.FloatField(blank=True, null=True)),
                ('ttm_rev', models.FloatField(blank=True, null=True)),
                ('mkt_cap', models.FloatField(blank=True, null=True)),
                ('ttm_ebitda', models.FloatField(blank=True, null=True)),
                ('ttm_capex', models.FloatField(blank=True, null=True)),
                ('net_debt', models.FloatField(blank=True, null=True)),
                ('roe', models.FloatField(blank=True, null=True)),
                ('cfps', models.FloatField(blank=True, null=True)),
                ('peg', models.FloatField(blank=True, null=True)),
                ('bps1fd12', models.FloatField(blank=True, null=True)),
                ('ebd1fd12', models.FloatField(blank=True, null=True)),
                ('evt1fd12', models.FloatField(blank=True, null=True)),
                ('eps1fd12', models.FloatField(blank=True, null=True)),
                ('sal1fd12', models.FloatField(blank=True, null=True)),
                ('cap1fd12', models.FloatField(blank=True, null=True)),
                ('environment', models.FloatField(blank=True, null=True)),
                ('social', models.FloatField(blank=True, null=True)),
                ('goverment', models.FloatField(blank=True, null=True)),
                ('assets_1yr', models.FloatField(blank=True, null=True)),
                ('cash', models.FloatField(blank=True, null=True)),
                ('current_asset', models.FloatField(blank=True, null=True)),
                ('equity', models.FloatField(blank=True, null=True)),
                ('ttm_cogs', models.FloatField(blank=True, null=True)),
                ('inventory', models.FloatField(blank=True, null=True)),
                ('ttm_eps', models.FloatField(blank=True, null=True)),
                ('ttm_gm', models.FloatField(blank=True, null=True)),
                ('income_tax', models.FloatField(blank=True, null=True)),
                ('pension_exp', models.FloatField(blank=True, null=True)),
                ('ppe_depreciation', models.FloatField(blank=True, null=True)),
                ('ppe_impairment', models.FloatField(blank=True, null=True)),
                ('mkt_cap_usd', models.FloatField(blank=True, null=True)),
                ('eps_lastq', models.FloatField(blank=True, null=True)),
                ('mkt_cap_ye', models.FloatField(blank=True, null=True)),
                ('ticker', models.ForeignKey(db_column='ticker', on_delete=django.db.models.deletion.CASCADE, related_name='data_fundamental_score_history_ticker', to='universe.universe')),
            ],
            options={
                'db_table': 'data_fundamental_score_history',
                'managed': True,
            },
        ),
    ]
