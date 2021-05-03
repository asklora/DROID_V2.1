from django.apps import AppConfig


class UserConfig(AppConfig):
    name = 'core.user'

    def ready(self):
        import core.user.signals
