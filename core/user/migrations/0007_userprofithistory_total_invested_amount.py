# Generated by Django 3.2.5 on 2021-09-01 04:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0006_userprofithistory'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofithistory',
            name='total_invested_amount',
            field=models.FloatField(default=0),
        ),
    ]
