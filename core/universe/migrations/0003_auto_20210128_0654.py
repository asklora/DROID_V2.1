# Generated by Django 3.1.4 on 2021-01-28 06:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0002_auto_20210126_0715'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='universeconsolidated',
            name='ticker_fullname',
        ),
        migrations.AlterField(
            model_name='universeconsolidated',
            name='is_active',
            field=models.BooleanField(default=False),
        ),
    ]
