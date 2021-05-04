from django.core.management.base import BaseCommand, CommandError
from core.user.models import TransactionHistory, Accountbalance
import json
from rest_framework import serializers


class AccountSerializer(serializers.ModelSerializer):

    class Meta:
        model = Accountbalance
        fields = '__all__'


class TransactionSerializer(serializers.ModelSerializer):

    class Meta:
        model = TransactionHistory
        fields = '__all__'


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '-b', '--backup', action='store_true', help='for backup')
        parser.add_argument(
            '-r', '--restore', action='store_true', help='for restore')

    def handle(self, *args, **options):
        with open('files/balance.json', 'w') as f:
            json.dump(AccountSerializer(
                Accountbalance.objects.all(), many=True).data, f)
            f.close()
        with open('files/transaction.json', 'w') as ftr:
            json.dump(TransactionSerializer(
                TransactionHistory.objects.all(), many=True).data, ftr)
            ftr.close()
