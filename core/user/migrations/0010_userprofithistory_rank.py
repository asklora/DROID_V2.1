# Generated by Django 3.2.5 on 2021-09-08 08:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0009_user_is_joined'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofithistory',
            name='rank',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
