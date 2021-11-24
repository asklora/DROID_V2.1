# Generated by Django 3.2.5 on 2021-08-05 04:10

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('channels_presence', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ErrorLog',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField()),
                ('updated', models.DateTimeField(editable=False)),
                ('error_description', models.TextField(blank=True, null=True)),
                ('error_traceback', models.TextField(blank=True, null=True)),
                ('error_message', models.TextField(blank=True, null=True)),
                ('error_function', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'log_error',
                'ordering': ['created'],
            },
        ),
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
        migrations.CreateModel(
            name='ChannelPresence',
            fields=[
            ],
            options={
                'proxy': True,
                'indexes': [],
                'constraints': [],
            },
            bases=('channels_presence.room',),
        ),
    ]
