# Generated by Django 3.2.5 on 2021-08-01 08:35

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('universe', '0010_remove_universe_yolo'),
        ('topstock', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='DLPAModel',
            fields=[
                ('model_filename', models.TextField(primary_key=True, serialize=False)),
                ('model_type', models.TextField(blank=True, null=True)),
                ('data_period', models.TextField(blank=True, null=True)),
                ('created', models.DateField(blank=True, null=True)),
                ('forward_date', models.DateField(blank=True, null=True)),
                ('forward_week', models.IntegerField(blank=True, null=True)),
                ('forward_dow', models.TextField(blank=True, null=True)),
                ('train_dow', models.TextField(blank=True, null=True)),
                ('best_train_acc', models.FloatField(blank=True, null=True)),
                ('best_valid_acc', models.FloatField(blank=True, null=True)),
                ('run_time_min', models.FloatField(blank=True, null=True)),
                ('train_num', models.IntegerField(blank=True, null=True)),
                ('cnn_kernel_size', models.IntegerField(blank=True, null=True)),
                ('batch_size', models.IntegerField(blank=True, null=True)),
                ('learning_rate', models.IntegerField(blank=True, null=True)),
                ('lookback', models.IntegerField(blank=True, null=True)),
                ('epoch', models.IntegerField(blank=True, null=True)),
                ('param_name_1', models.TextField(blank=True, null=True)),
                ('param_val_1', models.FloatField(blank=True, null=True)),
                ('param_name_2', models.TextField(blank=True, null=True)),
                ('param_val_2', models.FloatField(blank=True, null=True)),
                ('param_name_3', models.TextField(blank=True, null=True)),
                ('param_val_3', models.FloatField(blank=True, null=True)),
                ('param_name_4', models.TextField(blank=True, null=True)),
                ('param_val_4', models.TextField(blank=True, null=True)),
                ('param_name_5', models.TextField(blank=True, null=True)),
                ('param_val_5', models.TextField(blank=True, null=True)),
                ('num_bins', models.IntegerField(blank=True, null=True)),
                ('num_nans_to_skip', models.IntegerField(blank=True, null=True)),
                ('accuracy_for_embedding', models.IntegerField(blank=True, null=True)),
                ('candle_type_returnsX', models.TextField(blank=True, null=True)),
                ('candle_type_returnsY', models.TextField(blank=True, null=True)),
                ('candle_type_candles', models.TextField(blank=True, null=True)),
                ('seed', models.IntegerField(blank=True, null=True)),
                ('best_valid_epoch', models.IntegerField(blank=True, null=True)),
                ('best_train_epoch', models.IntegerField(blank=True, null=True)),
                ('pc_number', models.TextField(blank=True, null=True)),
                ('stock_percentage', models.FloatField(blank=True, null=True)),
                ('valid_num', models.IntegerField(blank=True, null=True)),
                ('test_num', models.IntegerField(blank=True, null=True)),
                ('num_periods_to_predict', models.IntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'dlpa_model',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='DLPAModelStock',
            fields=[
                ('uid', models.CharField(max_length=255, primary_key=True, serialize=False)),
                ('data_period', models.TextField(blank=True, null=True)),
                ('created', models.DateField(blank=True, null=True)),
                ('forward_date', models.DateField(blank=True, null=True)),
                ('spot_date', models.DateField(blank=True, null=True)),
                ('year', models.IntegerField(blank=True, null=True)),
                ('week', models.IntegerField(blank=True, null=True)),
                ('day_of_week', models.IntegerField(blank=True, null=True)),
                ('num_periods_to_predict', models.IntegerField(blank=True, null=True)),
                ('number_of_quantiles', models.IntegerField(blank=True, null=True)),
                ('predicted_quantile_1', models.IntegerField(blank=True, null=True)),
                ('signal_strength_1', models.FloatField(blank=True, null=True)),
                ('pc_number', models.TextField(blank=True, null=True)),
                ('currency_code', models.ForeignKey(blank=True, db_column='currency_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='dlpa_model_stock_currency_code', to='universe.currency')),
                ('model_filename', models.ForeignKey(blank=True, db_column='model_filename', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='dlpa_model_stock_model_filename', to='topstock.dlpamodel')),
                ('ticker', models.ForeignKey(blank=True, db_column='ticker', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='dlpa_model_stock_stock_ticker', to='universe.universe')),
            ],
            options={
                'db_table': 'dlpa_model_stock',
                'managed': True,
            },
        ),
        migrations.RemoveField(
            model_name='topstockmodelstock',
            name='currency_code',
        ),
        migrations.RemoveField(
            model_name='topstockmodelstock',
            name='model_filename',
        ),
        migrations.RemoveField(
            model_name='topstockmodelstock',
            name='ticker',
        ),
        migrations.RemoveField(
            model_name='topstockperformance',
            name='client_id',
        ),
        migrations.RemoveField(
            model_name='topstockperformance',
            name='currency_code',
        ),
        migrations.RemoveField(
            model_name='topstockperformance',
            name='subinterval',
        ),
        migrations.RemoveField(
            model_name='topstockweekly',
            name='client_id',
        ),
        migrations.RemoveField(
            model_name='topstockweekly',
            name='currency_code',
        ),
        migrations.RemoveField(
            model_name='topstockweekly',
            name='subinterval',
        ),
        migrations.RemoveField(
            model_name='topstockweekly',
            name='ticker',
        ),
        migrations.DeleteModel(
            name='Subinterval',
        ),
        migrations.DeleteModel(
            name='TopStockModel',
        ),
        migrations.DeleteModel(
            name='TopStockModelStock',
        ),
        migrations.DeleteModel(
            name='TopStockPerformance',
        ),
        migrations.DeleteModel(
            name='TopStockWeekly',
        ),
    ]
