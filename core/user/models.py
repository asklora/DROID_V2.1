from django.db import models
from django.contrib.auth.models import AbstractBaseUser, PermissionsMixin
from .manager import AppUserManager
import uuid
from core.universe.models import Currency
from django.db import IntegrityError
from django.db.models import (
    Sum
)
import base64
from core.djangomodule.models import BaseTimeStampModel
from core.djangomodule.general import nonetozero
from simple_history.models import HistoricalRecords


def generate_balance_id():
    r_id = base64.b64encode(uuid.uuid4().bytes).replace('=', '').decode()
    return r_id


def usermanagerprofile(instance, filename):
    ext = filename.split('.')[-1]
    filename = "%s.%s" % (uuid.uuid4(), ext)
    return '{0}_manager_profile_pic/{1}'.format(instance.username, filename)


class User(AbstractBaseUser, PermissionsMixin):
    WAIT, APPROVED ,UNVERIFIED,VERIFIED= 'in waiting list', 'approved','unverified','verified'
    status_choices = (
        (UNVERIFIED, 'unverified'),
        (VERIFIED, 'verified'),
        (WAIT, 'in waiting list'),
        (APPROVED, 'approved'),
    )

    email = models.EmailField(('email address'),null=True, blank=True)
    username = models.CharField(
        max_length=255, unique=True)
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
        max_length=255, null=True, blank=True, choices=status_choices, default=UNVERIFIED)

    USERNAME_FIELD = 'username'
    AUTH_FIELD_NAME = 'email'
    # REQUIRED_FIELDS = ['username']

    objects = AppUserManager()

    @property
    def is_small(self):
        try:
            user_client = self.client_user.all()[0]
            return user_client.extra_data["capital"] == "small"
        except AttributeError:
            return False
        except self.DoesNotExist:
            return False
        except KeyError:
            return False
        except Exception:
            return False
        
    @property
    def is_large(self):
        try:
            user_client = self.client_user.all()[0]
            return user_client.extra_data["capital"] == "large_margin"
        except AttributeError:
            return False
        except self.DoesNotExist:
            return False
        except KeyError:
            return False
        except Exception:
            return False

    @property
    def is_large_margin(self):
        try:
            user_client = self.client_user.all()[0]
            return user_client.extra_data["capital"] == "large_margin"
        except AttributeError:
            return False
        except self.DoesNotExist:
            return False
        except KeyError:
            return False
        except Exception:
            return False

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
        return round(self.user_balance.amount - self.total_pending_amount,2)

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
    def total_user_invested_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True,bot_id='STOCK_stock_0').aggregate(total=Sum('investment_amount'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0
    
    @property
    def total_bot_invested_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True).exclude(bot_id='STOCK_stock_0').aggregate(total=Sum('investment_amount'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0

    @property
    def total_invested_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True).aggregate(total=Sum('investment_amount'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0
    
    @property
    def total_stock_amount(self):
        order = self.user_position.filter(user_id=self,is_live=True).aggregate(total=Sum('current_inv_amt'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0

    @property
    def starting_amount(self):
        #USER STARTING AMOUNT
        wallet = self.user_balance.account_transaction.filter(
            transaction_detail__event='first deposit').aggregate(total=Sum('amount'))
        if wallet['total']:
            return wallet['total']
        return 0

    @property
    def current_total_investment_value(self):
        # USER all INVESTMENT VALUE UNREALIZED
        order = self.user_position.filter(user_id=self,is_live=True).aggregate(total=Sum('current_values'))
        if order['total']:
            result = round(order['total'], 2)
            return result
        return 0
    
    @property
    def total_pending_amount(self):
        pending_transaction=self.user_order.filter(status='pending').aggregate(total=Sum('amount'))
        if pending_transaction['total']:
            return pending_transaction['total']
        return 0
    
    @property
    def total_amount(self):
        return round(self.balance  + self.current_total_investment_value,2)
    
    
    @property
    def total_profit_amount(self):
        return round(self.total_amount - self.starting_amount,2)
    
    @property
    def total_profit_return(self):
        return round((self.total_amount / self.starting_amount) - 1,4)
    
    @property
    def total_fee_amount(self):
        transaction=self.user_balance.account_transaction.filter(transaction_detail__event__in=['fee']).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0
    
    @property
    def total_stamp_amount(self):
        transaction=self.user_balance.account_transaction.filter(transaction_detail__event__in=['stamp_duty']).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0
    
    @property
    def total_commission_amount(self):
        transaction=self.user_balance.account_transaction.filter(transaction_detail__event__in=['fee']).aggregate(total=Sum('amount'))
        transaction2=self.user_balance.account_transaction.filter(transaction_detail__event__in=['stamp_duty']).aggregate(total=Sum('amount'))
        if transaction['total']:
            if transaction2['total']:
                total2=transaction2['total']
            else:
                total2=0
            result = round(transaction['total'] -total2, 2)
            return result
        return 0

    
    

    class Meta:
        db_table = 'user_core'
        indexes = [models.Index(fields=['email','username',])]
        

    def __str__(self):
        if self.email:
            return self.email
        return str(self.id)


class Accountbalance(BaseTimeStampModel):
    balance_uid = models.CharField(
        primary_key=True, max_length=300, blank=True, editable=False, unique=True)
    user = models.OneToOneField(
        User, on_delete=models.CASCADE, related_name='user_balance', db_column='user_id')
    amount = models.FloatField(default=0)
    currency_code = models.ForeignKey(
        Currency, on_delete=models.DO_NOTHING, related_name='user_currency', default='USD', db_column='currency_code')
    history = HistoricalRecords(table_name='user_account_history')

    def __str__(self):
        if self.user.email:
            return self.user.email
        return self.balance_uid

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
                                    related_name='account_transaction', db_column='balance_uid')
    side = models.CharField(max_length=100, choices=type_choice)
    amount = models.FloatField(default=0)
    transaction_detail = models.JSONField(default=dict, null=True, blank=True)

    def __str__(self):
        if self.balance_uid.user.email:
            return f'{self.side} - {self.balance_uid.user.email}'
        return self.side

    class Meta:
        db_table = 'user_transaction'
