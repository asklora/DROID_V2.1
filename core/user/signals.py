from .models import TransactionHistory, Accountbalance,User,UserDepositHistory
from django.db.models.signals import post_save, pre_save,post_delete
from django.dispatch import receiver
from core.djangomodule.general import formatdigit
from bot.calculate_bot import update_monthly_deposit


@receiver(post_save, sender=TransactionHistory)
def transaction_add(sender, instance, created, **kwargs):
    """
    this function for decreasing and increasing the user balance
    instance is the object itself (TransactionHistory)
    """
    wallet = Accountbalance.objects.get(
        balance_uid=instance.balance_uid.balance_uid)
    trans_type = instance.side
    if created:
        if trans_type == 'debit':
            result = wallet.amount - instance.amount
        elif trans_type == 'credit':
            result = wallet.amount + instance.amount
        
        wallet.amount = result

        wallet.save()

@receiver(post_delete, sender=TransactionHistory)
def transaction_dec(sender, instance, **kwargs):
    """
    this function for decreasing and increasing the user balance
    instance is the object itself (TransactionHistory)
    """
    wallet = Accountbalance.objects.get(
        balance_uid=instance.balance_uid.balance_uid)
    trans_type = instance.side
    print('called return deposit')
    if trans_type == 'debit':
        result = wallet.amount + instance.amount
        wallet.amount = formatdigit(result, wallet.currency_code.is_decimal)
        wallet.save()
    elif trans_type == 'credit':
        result = wallet.amount - instance.amount
        wallet.amount = formatdigit(result, wallet.currency_code.is_decimal)
        wallet.save()


@receiver(post_save,sender=User)
def join_competition_lock_balance(sender, instance, created, **kwargs):

    if instance.is_joined and not UserDepositHistory.objects.filter(user_id=instance.id).exists():
        update_monthly_deposit(user_id=[instance.id])
