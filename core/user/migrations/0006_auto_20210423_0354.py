# Generated by Django 3.1.4 on 2021-04-23 03:54

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0005_accountbalance_transactionhistory'),
    ]

    operations = [
        migrations.AlterField(
            model_name='transactionhistory',
            name='created',
            field=models.DateTimeField(),
        ),
        migrations.AlterField(
            model_name='transactionhistory',
            name='updated',
            field=models.DateTimeField(editable=False),
        ),
    ]
