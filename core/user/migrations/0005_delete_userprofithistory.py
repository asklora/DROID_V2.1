# Generated by Django 3.2.5 on 2021-08-26 10:12

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0004_userprofithistory'),
    ]

    operations = [
        migrations.DeleteModel(
            name='UserProfitHistory',
        ),
    ]
