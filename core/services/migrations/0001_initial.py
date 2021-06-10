# Generated by Django 3.1.4 on 2021-06-10 05:24

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='ThirdpartyCredentials',
            fields=[
                ('created', models.DateTimeField()),
                ('updated', models.DateTimeField(editable=False)),
                ('services', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('token', models.CharField(blank=True, max_length=255, null=True)),
                ('base_url', models.CharField(blank=True, max_length=255, null=True)),
                ('username', models.CharField(blank=True, max_length=255, null=True)),
                ('password', models.CharField(blank=True, max_length=255, null=True)),
                ('extra_data', models.JSONField(blank=True, default=dict, null=True)),
            ],
            options={
                'db_table': 'services',
            },
        ),
    ]
