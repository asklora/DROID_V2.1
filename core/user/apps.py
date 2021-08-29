from django.apps import AppConfig

import signal

class UserConfig(AppConfig):
    name = 'core.user'

    def ready(self):
        import core.user.signals
