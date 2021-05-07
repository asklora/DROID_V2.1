from .models import TransactionHistory, Accountbalance
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from core.djangomodule.general import formatdigit


@receiver(post_save, sender=TransactionHistory)
def transaction(sender, instance, created, **kwargs):
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
            wallet.amount = formatdigit(result, wallet.currency_code.is_decimal)
            wallet.save()
        elif trans_type == 'credit':
            result = wallet.amount + instance.amount
            wallet.amount = formatdigit(result, wallet.currency_code.is_decimal)
            wallet.save()
