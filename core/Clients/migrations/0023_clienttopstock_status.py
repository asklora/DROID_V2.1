# Generated by Django 3.1.4 on 2021-06-02 06:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('Clients', '0022_auto_20210507_0357'),
    ]

    operations = [
        migrations.AddField(
            model_name='clienttopstock',
            name='status',
            field=models.TextField(blank=True, null=True),
        ),
    ]
