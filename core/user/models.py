from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from .manager import AppUserManager
from django.core.exceptions import ValidationError
import uuid
from django.utils import timezone
# from backendmodel.universe.models import Currency
from datetime import datetime, date
from django.db import IntegrityError
from django.conf import settings
import time
from django.db.models import (
    Case, When, Value,
    F, FloatField, ExpressionWrapper,
    Sum, Q, Lookup
    )
from general.helper import nonetozero
import jwt
import base64
from django.db.models.fields import Field


class NotEqual(Lookup):
    lookup_name = 'ne'

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        rhs, rhs_params = self.process_rhs(qn, connection)
        params = lhs_params + rhs_params
        return '%s <> %s' % (lhs, rhs), params


Field.register_lookup(NotEqual)


def validate_decimals(value):
    try:
        return round(float(value), 2)
    except Exception:
        raise ValidationError(
            ('%(value)s is not an integer or a float  number'), params={'value': value},)




def generate_balance_id():
    r_id = base64.b64encode(uuid.uuid4().bytes).replace('=', '').decode()
    return r_id


def usermanagerprofile(instance, filename):
    ext = filename.split('.')[-1]
    filename = "%s.%s" % (uuid.uuid4(), ext)
    return '{0}_manager_profile_pic/{1}'.format(instance.username, filename)


class CountryPhoneDial(models.Model):
    country_name = models.CharField(max_length=255, unique=True)
    country_name_english = models.CharField(
        max_length=255, null=True, blank=True)
    country_code_iso2 = models.CharField(max_length=10, null=True, blank=True)
    country_code_iso3 = models.CharField(max_length=10, null=True, blank=True)
    country_dial_code = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        managed = True
        db_table = 'country_dial'

    @property
    def country_long_name(self):
        if self.country_name_english == '' or self.country_name_english == None:
            return self.country_name
        return self.country_name_english

    @property
    def country_dial_format(self):
        formats = f'{self.country_long_name} [{self.country_dial}]'
        return formats

    def __str__(self):
        if self.country_dial_code:
            return f'{self.country_dial_code} {self.country_name}'
        return self.country_name


class User(AbstractBaseUser, PermissionsMixin):
    WAIT, APPROVED = 'in waiting list', 'approved'
    status_choices = (
        (WAIT, 'in waiting list'),
        (APPROVED, 'approved'),
    )

    email = models.EmailField(('email address'), unique=True)
    username = models.CharField(
        max_length=255, unique=True, blank=True, null=True)
    country_dial = models.ForeignKey(
        CountryPhoneDial, on_delete=models.CASCADE, related_name='user_country_dial', null=True, blank=True)
    phone = models.CharField(max_length=20, null=True, blank=True)
    address = models.TextField(blank=True, null=True)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    fullname = models.CharField(max_length=255, null=True, blank=True)
    country = models.CharField(max_length=255, null=True, blank=True)
    country_code = models.CharField(max_length=255, null=True, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    avatar = models.ImageField(
        upload_to=usermanagerprofile, null=True, blank=True)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=False)
    telegram_notif = models.BooleanField(default=False)
    telegram_chat_id = models.CharField(max_length=200, null=True, blank=True)
    date_joined = models.DateTimeField(auto_now_add=True)
    over_18 = models.BooleanField(default=False)
    account_type = models.TextField(null=True, blank=True)
    current_status = models.CharField(
        max_length=255, null=True, blank=True, choices=status_choices, default=WAIT)
    frac = models.BooleanField(default=False)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']

    objects = AppUserManager()

    @property
    def get_dial_phone(self):
        phones = list(self.phone)
        if phones[0] == '0':
            phones[0] = ''
            phone = ''.join(phones).strip()
            phone = self.country.country_dial + phone
            return phone
        return self.country_dial.country_dial_code+self.phone

    @property
    def name(self):
        return f'{self.first_name} {self.last_name}'

    def __str__(self):
        return self.email

    # @property
    # def get_current_assets(self):
    #     order = self.user_order.filter(status=False).aggregate(total=Sum(
    #         Case(When(~Q(currency_id=self.user_balance.currency.currency_code),
    #                   then=F('current_values')),
    #              default=F('current_values'),
    #              output_field=FloatField(),
    #              )))
    #     asset = noneToZero(order['total']) + self.user_balance.amount
    #     result = round(asset, 2)
    #     return result

    @property
    def balance(self):
        return self.user_balance.amount
    class Meta:
        db_table = 'user_core'


class Accountbalance(models.Model):
    balance_id = models.CharField(
        primary_key=True, max_length=300, blank=True, editable=False, unique=True)
    user = models.OneToOneField(
        User, on_delete=models.CASCADE, related_name='user_balance')
    amount = models.FloatField(default=0, validators=[validate_decimals])
    # currency = models.ForeignKey(
    #     Currency, on_delete=models.DO_NOTHING, related_name='user_currency', default='USD')
    last_updated = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.user.email

    def save(self, *args, **kwargs):
        if self.amount == None:
            self.amount = 0
        if not self.balance_id:
            self.balance_id = uuid.uuid4().hex
            # using your function as above or anything else
        success = False
        failures = 0
        while not success:
            try:
                super(Accountbalance, self).save(*args, **kwargs)
            except IntegrityError:
                failures += 1
                if failures > 5:  # or some other arbitrary cutoff point at which things are clearly wrong
                    raise KeyError
                else:
                    # looks like a collision, try another random value
                    self.balance_id = uuid.uuid4().hex
            else:
                success = True

    class Meta:
        db_table = 'user_account_balance'


class TransactionHistory(models.Model):
    C = 'credit'
    D = 'debit'
    type_choice = (
        (C, 'credit'),
        (D, 'debit')
    )
    tr_id = models.CharField(max_length=500, editable=False)
    balance_id = models.ForeignKey(Accountbalance, on_delete=models.CASCADE,
                                   related_name='account_trasaction', db_column='balanceId')
    tr_type = models.CharField(max_length=100, choices=type_choice)
    tr_amount = models.FloatField(default=0, validators=[validate_decimals])
    description = models.TextField()
    created = models.DateTimeField(auto_now=True)
    updated = models.DateTimeField(auto_now_add=True)
    bot_transaction = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self):
        return f'{self.tr_type} - {self.balance_id.user.email}'

    def save(self, *args, **kwargs):
        if self.created == None:
            self.created = datetime.now()
        payload = {
            'user': self.balance_id.user.email,
            'transaction_type': self.tr_type,
            'amount': self.tr_amount,
            'balance_id': self.balance_id.balance_id,
            'description': self.description,
            'created': str(self.created)
        }
        token_id = jwt.encode(payload, settings.SECRET_KEY,
                              algorithm='HS256').decode('utf-8')
        token_id = token_id.replace('.', '|')
        self.tr_amount = round(self.tr_amount, 2)
        self.tr_id = token_id
        super(TransactionHistory, self).save(*args, **kwargs)

    class Meta:
        db_table = 'user_transaction'


class UserLogHistory(models.Model):
    user = models.ForeignKey(
        User, on_delete=models.CASCADE, related_name='user_log_history')
    name = models.CharField(max_length=255, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    login_time = models.TimeField(max_length=255, null=True, blank=True)
    login_date = models.DateField(max_length=255, null=True, blank=True)
    timeZone = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        date = str(self.login_time)
        return f'{self.email} @{date}'

    class Meta:
        db_table = 'user_logs'
        get_latest_by = "login_time"
        ordering = ['-login_time', '-login_date']
        verbose_name = 'USER LOG HISTORIE'
        verbose_name_plural = 'USER LOG HISTORIES'


class UserActivity(User):

    class Meta:
        proxy = True
        verbose_name = 'User activitie'
        verbose_name_plural = 'User activities'
