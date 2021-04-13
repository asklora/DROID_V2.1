from django.apps import AppConfig


class OrdersConfig(AppConfig):
    name = 'core.orders'
    def ready(self):
        import core.orders.signals
