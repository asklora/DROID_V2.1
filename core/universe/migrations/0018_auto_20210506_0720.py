# Generated by Django 3.1.4 on 2021-05-06 07:20

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0017_auto_20210506_0707'),
    ]

    operations = [
        migrations.RenameField(
            model_name='currency',
            old_name='hanwha_close_schedule',
            new_name='hedge_schedule',
        ),
    ]
