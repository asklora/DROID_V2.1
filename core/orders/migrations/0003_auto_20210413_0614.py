# Generated by Django 3.1.4 on 2021-04-13 06:14

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('orders', '0002_auto_20210413_0605'),
    ]

    operations = [
        migrations.AlterModelTable(
            name='positionperformance',
            table='order_position_performance',
        ),
    ]
