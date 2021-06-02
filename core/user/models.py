from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from .manager import AppUserManager
from django.core.exceptions import ValidationError
import uuid
from django.utils import timezone
from core.universe.models import Currency, Country
from datetime import datetime, date
from django.db import IntegrityError
from django.conf import settings
import time
from django.db.models import (
    Case, When, Value,
    F, FloatField, ExpressionWrapper,
    Sum, Q, Lookup
)
import base64
from core.djangomodule.models import BaseTimeStampModel
from core.djangomodule.general import nonetozero


def generate_balance_id():
    r_id = base64.b64encode(uuid.uuid4().bytes).replace('=', '').decode()
    return r_id


def usermanagerprofile(instance, filename):
    ext = filename.split('.')[-1]
    filename = "%s.%s" % (uuid.uuid4(), ext)
    return '{0}_manager_profile_pic/{1}'.format(instance.username, filename)


class User(AbstractBaseUser, PermissionsMixin):
    WAIT, APPROVED = 'in waiting list', 'approved'
    status_choices = (
        (WAIT, 'in waiting list'),
        (APPROVED, 'approved'),
    )

    email = models.EmailField(('email address'), unique=True)
    username = models.CharField(
        max_length=255, unique=True, blank=True, null=True)
    phone = models.CharField(max_length=20, null=True, blank=True)
    address = models.TextField(blank=True, null=True)
    first_name = models.CharField(max_length=255, null=True, blank=True)
    last_name = models.CharField(max_length=255, null=True, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    avatar = models.ImageField(
        upload_to=usermanagerprofile, null=True, blank=True)
    is_staff = models.BooleanField(default=False)
    is_active = models.BooleanField(default=False)
    date_joined = models.DateTimeField(auto_now_add=True)
    current_status = models.CharField(
        max_length=255, null=True, blank=True, choices=status_choices, default=WAIT)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']

    objects = AppUserManager()

    @property
    def is_small(self):
        user_client = self.client_user.all()[0]
        return user_client.extra_data["capital"] == "small"

    @property
    def is_large(self):
        user_client = self.client_user.all()[0]

        return user_client.extra_data["capital"] == "large"

    @property
    def is_large_margin(self):
        user_client = self.client_user.all()[0]
        return user_client.extra_data["capital"] == "large_margin"

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

    @property
    def current_assets(self):
        order = self.user_position.filter(
            is_live=True).aggregate(total=Sum('current_values'))
        asset = nonetozero(order['total']) + self.user_balance.amount
        result = round(asset, 2)
        return result

    

    @property
    def balance(self):
        return self.user_balance.amount

    @property
    def wallet(self):
        return self.user_balance
    
    @property
    def currency(self) -> str:
        return self.user_balance.currency_code.currency_code

    
    @property
    def position_live(self):
        position = self.user_position.filter(is_live=True).count()
        if position >= 1:
            return position
        return 0
    
    
    @property
    def position_expired(self):
        position = self.user_position.filter(is_live=False).count()
        if position >= 1:
            return position
        return 0
    
    @property
    def total_invested_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True).aggregate(total=Sum('investment_amount'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0
    

    @property
    def starting_amount(self):
        wallet = self.user_balance.account_trasaction.filter(
            transaction_detail__event='first deposit').aggregate(total=Sum('amount'))
        if wallet['total']:
            return wallet['total']
        return 0

    @property
    def current_total_invested_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True).aggregate(total=Sum(F('investment_amount')- F('bot_cash_balance')))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0
    
    @property
    def total_amount(self):
        return round(self.balance + self.total_invested_amount,2)
    
    @property
    def total_profit_amount(self):
        return round(self.total_amount - self.starting_amount,2)
    
    @property
    def total_profit_return(self):
        return round(self.total_amount / self.starting_amount - 1,4)
    

    class Meta:
        db_table = 'user_core'

    def __str__(self):
        return self.email


class Accountbalance(BaseTimeStampModel):
    balance_uid = models.CharField(
        primary_key=True, max_length=300, blank=True, editable=False, unique=True)
    user = models.OneToOneField(
        User, on_delete=models.CASCADE, related_name='user_balance', db_column='user_id')
    amount = models.FloatField(default=0)
    currency_code = models.ForeignKey(
        Currency, on_delete=models.DO_NOTHING, related_name='user_currency', default='USD', db_column='currency_code')

    def __str__(self):
        return self.user.email

    def save(self, *args, **kwargs):
        if self.amount == None:
            self.amount = 0
        if not self.balance_uid:
            self.balance_uid = uuid.uuid4().hex
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
                    self.balance_uid = uuid.uuid4().hex
            else:
                success = True

    class Meta:
        db_table = 'user_account_balance'


class TransactionHistory(BaseTimeStampModel):
    C = 'credit'
    D = 'debit'
    type_choice = (
        (C, 'credit'),
        (D, 'debit')
    )
    balance_uid = models.ForeignKey(Accountbalance, on_delete=models.CASCADE,
                                    related_name='account_trasaction', db_column='balance_uid')
    side = models.CharField(max_length=100, choices=type_choice)
    amount = models.FloatField(default=0)
    transaction_detail = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self):
        return f'{self.tr_type} - {self.balance_id.user.email}'

    class Meta:
        db_table = 'user_transaction'
