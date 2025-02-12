from django.core.management.base import BaseCommand
from core.user.models import User
from core.djangomodule.crudlib.user import sync_delete_user
from general.firestore_query import delete_firestore_user
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        users  = User.objects.filter(current_status__in=['unverified','verified'])
        for user in users:
            payload = {'username':user.username}
            sync_delete_user(payload)