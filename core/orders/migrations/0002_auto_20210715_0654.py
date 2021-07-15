# Generated by Django 3.1.4 on 2021-07-15 06:54

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('universe', '0001_initial'),
        ('orders', '0001_initial'),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name='orderposition',
            name='user_id',
            field=models.ForeignKey(db_column='user_id', on_delete=django.db.models.deletion.CASCADE, related_name='user_position', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddField(
            model_name='orderfee',
            name='order_uid',
            field=models.ForeignKey(db_column='order_uid', on_delete=django.db.models.deletion.CASCADE, related_name='orders_fee_orders', to='orders.order'),
        ),
        migrations.AddField(
            model_name='order',
            name='ticker',
            field=models.ForeignKey(db_column='ticker', on_delete=django.db.models.deletion.CASCADE, related_name='symbol_order', to='universe.universe'),
        ),
        migrations.AddField(
            model_name='order',
            name='user_id',
            field=models.ForeignKey(db_column='user_id', on_delete=django.db.models.deletion.CASCADE, related_name='user_order', to=settings.AUTH_USER_MODEL),
        ),
        migrations.AddIndex(
            model_name='positionperformance',
            index=models.Index(fields=['created', 'position_uid', 'order_uid'], name='orders_posi_created_641a2f_idx'),
        ),
        migrations.AddIndex(
            model_name='orderposition',
            index=models.Index(fields=['user_id', 'ticker'], name='orders_posi_user_id_729cf6_idx'),
        ),
    ]