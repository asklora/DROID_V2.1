# Generated by Django 3.1.4 on 2021-01-29 10:19

from django.db import migrations, models
import django.db.models.deletion
import django.db.models.manager


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0009_delete_universeconsolidated'),
    ]

    operations = [
        migrations.CreateModel(
            name='UniverseConsolidated',
            fields=[
                ('uid', models.CharField(editable=False, max_length=20, primary_key=True, serialize=False)),
                ('origin_ticker', models.CharField(max_length=10)),
                ('is_active', models.BooleanField(default=True)),
                ('created', models.DateField(blank=True, null=True)),
                ('updated', models.DateField(blank=True, null=True)),
                ('isin', models.CharField(blank=True, max_length=500, null=True)),
                ('use_isin', models.BooleanField(default=False)),
                ('cusip', models.CharField(blank=True, max_length=500, null=True)),
                ('use_cusip', models.BooleanField(default=False)),
                ('sedol', models.CharField(blank=True, max_length=500, null=True)),
                ('use_sedol', models.BooleanField(default=False)),
                ('use_manual', models.BooleanField(default=False)),
                ('permid', models.CharField(blank=True, max_length=500, null=True)),
                ('consolidated_ticker', models.CharField(blank=True, max_length=10, null=True)),
                ('source_id', models.ForeignKey(blank=True, db_column='source_id', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='universe_source_id', to='universe.source')),
            ],
            options={
                'db_table': 'universe_consolidated',
                'managed': True,
            },
            managers=[
                ('ingestion_manager', django.db.models.manager.Manager()),
            ],
        ),
    ]
