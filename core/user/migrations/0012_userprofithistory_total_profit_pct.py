# Generated by Django 3.2.5 on 2021-09-10 04:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0011_user_gender'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofithistory',
            name='total_profit_pct',
            field=models.FloatField(default=0),
        ),
    ]
