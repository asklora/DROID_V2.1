# Generated by Django 3.1.4 on 2021-06-03 05:18

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('Clients', '0024_clienttopstock_week_of_year'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='clienttopstock',
            name='status',
        ),
    ]
