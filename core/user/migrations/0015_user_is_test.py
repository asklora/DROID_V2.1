# Generated by Django 3.2.5 on 2021-09-10 07:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('user', '0014_auto_20210910_0503'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='is_test',
            field=models.BooleanField(default=False),
        ),
    ]
