# Generated by Django 3.1.4 on 2021-06-30 09:10

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0002_auto_20210630_0558'),
    ]

    operations = [
        migrations.RenameField(
            model_name='universeratingdetailhistory',
            old_name='environment_minmax_industry_code',
            new_name='environment_minmax_industry',
        ),
        migrations.RenameField(
            model_name='universeratingdetailhistory',
            old_name='goverment_minmax_industry_code',
            new_name='goverment_minmax_industry',
        ),
        migrations.RenameField(
            model_name='universeratingdetailhistory',
            old_name='social_minmax_industry_code',
            new_name='social_minmax_industry',
        ),
    ]