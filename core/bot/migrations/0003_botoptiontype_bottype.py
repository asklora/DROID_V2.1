# Generated by Django 3.1.4 on 2021-03-24 03:29

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('bot', '0002_botstatistic'),
    ]

    operations = [
        migrations.CreateModel(
            name='BotType',
            fields=[
                ('bot_type', models.TextField(primary_key=True, serialize=False)),
                ('bot_name', models.TextField(blank=True, null=True)),
            ],
            options={
                'db_table': 'bot_type',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='BotOptionType',
            fields=[
                ('bot_id', models.TextField(primary_key=True, serialize=False)),
                ('bot_option_type', models.DateField(blank=True, null=True)),
                ('time_to_exp', models.FloatField(blank=True, null=True)),
                ('bot_type', models.ForeignKey(db_column='bot_type', on_delete=django.db.models.deletion.CASCADE, related_name='bot_option_type_bot_type', to='bot.bottype')),
            ],
            options={
                'db_table': 'bot_option_type',
                'managed': True,
            },
        ),
    ]
