# Generated by Django 3.1.4 on 2021-03-16 08:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0013_universe_ticker_symbol'),
    ]

    operations = [
        migrations.AddField(
            model_name='universeconsolidated',
            name='has_data',
            field=models.BooleanField(default=True),
        ),
    ]
