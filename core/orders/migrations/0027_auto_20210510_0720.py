# Generated by Django 3.1.4 on 2021-05-10 07:20

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('orders', '0026_auto_20210510_0714'),
    ]

    operations = [
        migrations.RenameField(
            model_name='positionperformance',
            old_name='balance',
            new_name='margin_balance',
        ),
    ]
