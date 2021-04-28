from django.core.management.base import BaseCommand, CommandError
from core.user.models import User


class Command(BaseCommand):

    def handle(self, *args, **options):
        user = User.objects.get(email='asklora@loratechai.com')
        print(user.client_user.all()[0].client.client_uid)
