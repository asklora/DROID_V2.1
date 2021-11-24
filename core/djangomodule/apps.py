import logging
from django.apps import AppConfig, apps
from .asyncorm.utils import patch_manager


class DjangoModuleConfig(AppConfig):
    name = 'core.djangomodule'

    def ready(self):
        logging.info('AsyncORM: patching models')
        for model in apps.get_models(include_auto_created=True):
            patch_manager(model)