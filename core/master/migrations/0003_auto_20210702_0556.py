# Generated by Django 3.1.4 on 2021-07-02 05:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('master', '0002_auto_20210630_0555'),
    ]

    operations = [
        migrations.AddField(
            model_name='latestprice',
            name='latest_price',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='latestprice',
            name='volume',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
