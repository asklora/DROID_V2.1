# Generated by Django 3.2.5 on 2021-08-10 05:06

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0002_auto_20210809_0427'),
    ]

    operations = [
        migrations.AddField(
            model_name='universeratinghistory',
            name='recbuy',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='universeratinghistory',
            name='recsell',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
