# Generated by Django 3.1.4 on 2021-07-14 06:42

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0007_auto_20210713_0808'),
    ]

    operations = [
        migrations.AddField(
            model_name='universerating',
            name='classic_vol',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
