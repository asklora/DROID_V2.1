from django.apps import AppConfig


class UniverseConfig(AppConfig):
    name = 'core.universe'
    
    def ready(self):
        import core.universe.signals
