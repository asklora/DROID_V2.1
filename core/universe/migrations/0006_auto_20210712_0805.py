# Generated by Django 3.1.4 on 2021-07-12 08:05

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0005_universe_error'),
    ]

    operations = [
        migrations.RenameField(
            model_name='universeratingdetailhistory',
            old_name='tri_minmax_currency_code',
            new_name='tri_robust_scale',
        ),
        migrations.RemoveField(
            model_name='universeratingdetailhistory',
            name='tri_minmax_industry',
        ),
    ]
