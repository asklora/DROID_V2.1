# Generated by Django 3.2.5 on 2021-07-28 07:14

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('orders', '0003_orderposition_bot_cash_dividend'),
        ('services', '0002_channelpresence'),
    ]

    operations = [
        migrations.CreateModel(
            name='HedgeLogger',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField()),
                ('updated', models.DateTimeField(editable=False)),
                ('log_type', models.CharField(max_length=255)),
                ('status', models.CharField(default='pending', max_length=50)),
                ('error_log', models.TextField(blank=True, null=True)),
                ('order_uid', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='position_hedge_log', to='orders.orderposition')),
            ],
            options={
                'abstract': False,
            },
        ),
    ]