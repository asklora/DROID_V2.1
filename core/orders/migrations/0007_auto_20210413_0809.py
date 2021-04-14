# Generated by Django 3.1.4 on 2021-04-13 08:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('orders', '0006_auto_20210413_0757'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='placed',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='order',
            name='placed_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='order',
            name='status',
            field=models.CharField(blank=True, default='pending', max_length=10, null=True),
        ),
    ]
