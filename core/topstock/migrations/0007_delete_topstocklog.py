# Generated by Django 3.1.4 on 2021-06-03 04:25

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('topstock', '0006_topstocklog'),
    ]

    operations = [
        migrations.DeleteModel(
            name='TopStockLog',
        ),
    ]
