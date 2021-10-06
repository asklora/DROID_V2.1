from django.core.management.base import BaseCommand
from core.user.models import User
from general.firestore_query import delete_firestore_user
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        users  = User.objects.filter(current_status='verified')
        for user in users:
            user_id=user.id
            user.delete()
            delete_firestore_user(user_id)