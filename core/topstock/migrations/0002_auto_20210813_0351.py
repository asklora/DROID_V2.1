# Generated by Django 3.2.5 on 2021-08-13 03:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('topstock', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dlpamodel',
            name='created',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='dlpamodelstock',
            name='created',
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]