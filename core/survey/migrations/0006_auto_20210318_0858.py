# Generated by Django 3.1.4 on 2021-03-18 08:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('survey', '0005_auto_20210318_0559'),
    ]

    operations = [
        migrations.AlterField(
            model_name='surveyresults',
            name='email',
            field=models.CharField(blank=True, default=None, max_length=128, null=True, unique=True),
        ),
    ]
