# Generated by Django 3.1.4 on 2021-01-29 07:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Clients', '0002_auto_20210128_1039'),
    ]

    operations = [
        migrations.AlterField(
            model_name='client',
            name='uid',
            field=models.CharField(editable=False, max_length=255, primary_key=True, serialize=False),
        ),
    ]
