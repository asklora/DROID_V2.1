# Generated by Django 3.1.4 on 2021-06-22 09:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('master', '0011_auto_20210326_0942'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='masterohlcvtr',
            index=models.Index(fields=['ticker', 'currency_code'], name='master_ohlc_ticker_10f263_idx'),
        ),
    ]
