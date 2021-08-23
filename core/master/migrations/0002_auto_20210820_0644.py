# Generated by Django 3.2.5 on 2021-08-20 06:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('master', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='datafundamentalscore',
            name='cash',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='current_asset',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='eps_lastq',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='equity',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='income_tax',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='inventory',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='mkt_cap_usd',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='pension_exp',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='ppe_depreciation',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='ppe_impairment',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='total_asset',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='ttm_cogs',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='ttm_eps',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='datafundamentalscore',
            name='ttm_gm',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='worldscopesummary',
            name='fn_1451',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='worldscopesummary',
            name='fn_18274',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='worldscopesummary',
            name='fn_18810',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='worldscopesummary',
            name='fn_2401',
            field=models.FloatField(blank=True, null=True),
        ),
    ]