# Generated by Django 3.2.5 on 2021-09-01 05:57

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0007_userprofithistory_total_invested_amount'),
    ]

    operations = [
        migrations.RenameField(
            model_name='userprofithistory',
            old_name='total_invested_amount',
            new_name='daily_invested_amount',
        ),
    ]
