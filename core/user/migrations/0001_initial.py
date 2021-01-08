# Generated by Django 3.1.4 on 2021-01-08 07:55

import core.user.models
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('auth', '0012_alter_user_first_name_max_length'),
    ]

    operations = [
        migrations.CreateModel(
            name='User',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('password', models.CharField(max_length=128, verbose_name='password')),
                ('last_login', models.DateTimeField(blank=True, null=True, verbose_name='last login')),
                ('is_superuser', models.BooleanField(default=False, help_text='Designates that this user has all permissions without explicitly assigning them.', verbose_name='superuser status')),
                ('email', models.EmailField(max_length=254, unique=True, verbose_name='email address')),
                ('username', models.CharField(blank=True, max_length=255, null=True, unique=True)),
                ('phone', models.CharField(blank=True, max_length=20, null=True)),
                ('address', models.TextField(blank=True, null=True)),
                ('first_name', models.CharField(blank=True, max_length=255, null=True)),
                ('last_name', models.CharField(blank=True, max_length=255, null=True)),
                ('fullname', models.CharField(blank=True, max_length=255, null=True)),
                ('country', models.CharField(blank=True, max_length=255, null=True)),
                ('country_code', models.CharField(blank=True, max_length=255, null=True)),
                ('birth_date', models.DateField(blank=True, null=True)),
                ('avatar', models.ImageField(blank=True, null=True, upload_to=core.user.models.usermanagerprofile)),
                ('is_staff', models.BooleanField(default=False)),
                ('is_active', models.BooleanField(default=False)),
                ('telegram_notif', models.BooleanField(default=False)),
                ('telegram_chat_id', models.CharField(blank=True, max_length=200, null=True)),
                ('date_joined', models.DateTimeField(auto_now_add=True)),
                ('over_18', models.BooleanField(default=False)),
                ('account_type', models.TextField(blank=True, null=True)),
                ('current_status', models.CharField(blank=True, choices=[('in waiting list', 'in waiting list'), ('approved', 'approved')], default='in waiting list', max_length=255, null=True)),
                ('frac', models.BooleanField(default=False)),
            ],
            options={
                'db_table': 'user_core',
            },
        ),
        migrations.CreateModel(
            name='Accountbalance',
            fields=[
                ('balance_id', models.CharField(blank=True, editable=False, max_length=300, primary_key=True, serialize=False, unique=True)),
                ('amount', models.FloatField(default=0, validators=[core.user.models.validate_decimals])),
                ('last_updated', models.DateTimeField(auto_now_add=True)),
                ('user', models.OneToOneField(on_delete=django.db.models.deletion.CASCADE, related_name='user_balance', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'db_table': 'user_account_balance',
            },
        ),
        migrations.CreateModel(
            name='CountryPhoneDial',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('country_name', models.CharField(max_length=255, unique=True)),
                ('country_name_english', models.CharField(blank=True, max_length=255, null=True)),
                ('country_code_iso2', models.CharField(blank=True, max_length=10, null=True)),
                ('country_code_iso3', models.CharField(blank=True, max_length=10, null=True)),
                ('country_dial_code', models.CharField(blank=True, max_length=255, null=True)),
            ],
            options={
                'db_table': 'country_dial',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='UserLogHistory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(blank=True, max_length=255, null=True)),
                ('email', models.CharField(blank=True, max_length=255, null=True)),
                ('login_time', models.TimeField(blank=True, max_length=255, null=True)),
                ('login_date', models.DateField(blank=True, max_length=255, null=True)),
                ('timeZone', models.CharField(blank=True, max_length=255, null=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='user_log_history', to=settings.AUTH_USER_MODEL)),
            ],
            options={
                'verbose_name': 'USER LOG HISTORIE',
                'verbose_name_plural': 'USER LOG HISTORIES',
                'db_table': 'user_logs',
                'ordering': ['-login_time', '-login_date'],
                'get_latest_by': 'login_time',
            },
        ),
        migrations.CreateModel(
            name='TransactionHistory',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tr_id', models.CharField(editable=False, max_length=500)),
                ('tr_type', models.CharField(choices=[('credit', 'credit'), ('debit', 'debit')], max_length=100)),
                ('tr_amount', models.FloatField(default=0, validators=[core.user.models.validate_decimals])),
                ('description', models.TextField()),
                ('created', models.DateTimeField(auto_now=True)),
                ('updated', models.DateTimeField(auto_now_add=True)),
                ('bot_transaction', models.CharField(blank=True, max_length=500, null=True)),
                ('balance_id', models.ForeignKey(db_column='balanceId', on_delete=django.db.models.deletion.CASCADE, related_name='account_trasaction', to='user.accountbalance')),
            ],
            options={
                'db_table': 'user_transaction',
            },
        ),
        migrations.AddField(
            model_name='user',
            name='country_dial',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='user_country_dial', to='user.countryphonedial'),
        ),
        migrations.AddField(
            model_name='user',
            name='groups',
            field=models.ManyToManyField(blank=True, help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.', related_name='user_set', related_query_name='user', to='auth.Group', verbose_name='groups'),
        ),
        migrations.AddField(
            model_name='user',
            name='user_permissions',
            field=models.ManyToManyField(blank=True, help_text='Specific permissions for this user.', related_name='user_set', related_query_name='user', to='auth.Permission', verbose_name='user permissions'),
        ),
        migrations.CreateModel(
            name='UserActivity',
            fields=[
            ],
            options={
                'verbose_name': 'User activitie',
                'verbose_name_plural': 'User activities',
                'proxy': True,
                'indexes': [],
                'constraints': [],
            },
            bases=('user.user',),
        ),
    ]
