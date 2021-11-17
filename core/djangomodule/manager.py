

from django.db.models.manager import BaseManager

from .asyncorm.query import QuerySetAsync


class AsyncManager(BaseManager.from_queryset(QuerySetAsync)):
    pass