# Generated by Django 3.1.4 on 2021-01-29 07:17

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('Clients', '0004_auto_20210129_0716'),
    ]

    operations = [
        migrations.AlterField(
            model_name='userclient',
            name='client',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='client_related', to='Clients.client'),
        ),
    ]
