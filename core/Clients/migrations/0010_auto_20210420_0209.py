# Generated by Django 3.1.4 on 2021-04-20 02:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Clients', '0009_auto_20210416_0457'),
    ]

    operations = [
        migrations.AlterField(
            model_name='client',
            name='created',
            field=models.DateTimeField(),
        ),
        migrations.AlterField(
            model_name='clienttopstock',
            name='created',
            field=models.DateTimeField(),
        ),
        migrations.AlterField(
            model_name='universeclient',
            name='created',
            field=models.DateTimeField(),
        ),
        migrations.AlterField(
            model_name='userclient',
            name='created',
            field=models.DateTimeField(),
        ),
    ]
