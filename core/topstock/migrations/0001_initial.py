# Generated by Django 3.1.4 on 2021-07-15 06:15

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('Clients', '0001_initial'),
        ('universe', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='Subinterval',
            fields=[
                ('subinterval', models.PositiveIntegerField(primary_key=True, serialize=False)),
                ('spot_date', models.DateField(blank=True, null=True)),
                ('date_initial', models.IntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'subinterval',
            },
        ),
        migrations.CreateModel(
            name='TopStockModel',
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
                ('test_acc_1', models.FloatField(blank=True, null=True)),
                ('test_acc_2', models.FloatField(blank=True, null=True)),
                ('test_acc_3', models.FloatField(blank=True, null=True)),
                ('test_acc_4', models.FloatField(blank=True, null=True)),
                ('test_acc_5', models.FloatField(blank=True, null=True)),
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
                ('should_use', models.BooleanField(blank=True, default=False, null=True)),
                ('long_term', models.BooleanField(blank=True, default=False, null=True)),
                ('train_len', models.IntegerField(blank=True, null=True)),
                ('valid_len', models.IntegerField(blank=True, null=True)),
            ],
            options={
                'db_table': 'top_stock_models',
            },
        ),
        migrations.CreateModel(
            name='TopStockWeekly',
            fields=[
                ('uid', models.CharField(editable=False, max_length=200, primary_key=True, serialize=False, unique=True)),
                ('created', models.DateField(blank=True, null=True)),
                ('spot_date', models.DateField(blank=True, null=True)),
                ('forward_date', models.DateField(blank=True, null=True)),
                ('rank', models.IntegerField(blank=True, null=True)),
                ('types', models.CharField(blank=True, max_length=50, null=True)),
                ('prediction_period', models.CharField(blank=True, max_length=200, null=True)),
                ('spot_price', models.FloatField(blank=True, null=True)),
                ('spot_tri', models.FloatField(blank=True, null=True)),
                ('forward_tri', models.FloatField(blank=True, null=True)),
                ('forward_return', models.FloatField(blank=True, null=True, verbose_name='absolute_return')),
                ('index_forward_return', models.FloatField(blank=True, null=True, verbose_name='ewportperfomance')),
                ('index_spot_price', models.FloatField(blank=True, null=True)),
                ('index_forward_price', models.FloatField(blank=True, null=True)),
                ('client_id', models.ForeignKey(db_column='client_id', on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_weekly_client_id', to='Clients.client')),
                ('currency_code', models.ForeignKey(db_column='currency_code', on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_weekly_currency_code', to='universe.currency')),
                ('subinterval', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_weekly_subinterval', to='topstock.subinterval')),
                ('ticker', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_weekly_ticker', to='universe.universe')),
            ],
            options={
                'db_table': 'top_stock_weekly',
            },
        ),
        migrations.CreateModel(
            name='TopStockPerformance',
            fields=[
                ('uid', models.CharField(editable=False, max_length=200, primary_key=True, serialize=False, unique=True)),
                ('types', models.CharField(blank=True, max_length=200, null=True)),
                ('spot_date', models.DateField(blank=True, null=True)),
                ('forward_date', models.DateField(blank=True, null=True)),
                ('index_spot_price', models.FloatField(blank=True, null=True)),
                ('index_forward_price', models.FloatField(blank=True, null=True)),
                ('index_forward_return', models.FloatField(blank=True, null=True)),
                ('wts_tri', models.FloatField(blank=True, null=True)),
                ('index_tri', models.FloatField(blank=True, null=True)),
                ('average_forward_return', models.FloatField(blank=True, null=True)),
                ('week_status', models.CharField(blank=True, max_length=200, null=True)),
                ('client_id', models.ForeignKey(db_column='client_id', on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_performance_client_id', to='Clients.client')),
                ('currency_code', models.ForeignKey(db_column='currency_code', on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_performance_currency_code', to='universe.currency')),
                ('subinterval', models.ForeignKey(db_column='subinterval', on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_performance_subinterval', to='topstock.subinterval')),
            ],
            options={
                'db_table': 'top_stock_performance',
            },
        ),
        migrations.CreateModel(
            name='TopStockModelStock',
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
                ('currency_code', models.ForeignKey(blank=True, db_column='currency_code', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_models_stock_currency_code', to='universe.currency')),
                ('model_filename', models.ForeignKey(blank=True, db_column='model_filename', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_models_stock_model_filename', to='topstock.topstockmodel')),
                ('ticker', models.ForeignKey(blank=True, db_column='ticker', null=True, on_delete=django.db.models.deletion.CASCADE, related_name='top_stock_models_stock_ticker', to='universe.universe')),
            ],
            options={
                'db_table': 'top_stock_models_stock',
            },
        ),
    ]
