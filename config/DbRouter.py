class AuroraRouters:
    """
    A router to control all database operations on models in the
    auth and contenttypes applications.
    """
    route_app_labels = {
        'bot',
        'portfolio',
        'services',
        'universe',
        'user',

    }
    # mongo_app_labels = {
    #     'hot_data',
    # }

    def db_for_read(self, model, **hints):
        """
        Attempts to read auth and contenttypes models go to auth_db.
        """
        if model._meta.app_label in self.route_app_labels:
            # print('aurora')
            return 'aurora_read'
        # if model._meta.app_label in self.mongo_app_labels:
        #     print('mongo')
        #     return 'mongo'
        return 'default'

    def db_for_write(self, model, **hints):
        """
        Attempts to write auth and contenttypes models go to auth_db.
        """
        if model._meta.app_label in self.route_app_labels:
            return 'aurora_write'
        if model._meta.app_label in self.mongo_app_labels:
            return 'mongo'
        return 'default'

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the auth or contenttypes apps is
        involved.
        """
        db_set = {'default', 'aurora_read', 'aurora_write'}
        if obj1._state.db in db_set and obj2._state.db in db_set:
            return True
        return 'default'

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the auth and contenttypes apps only appear in the
        'auth_db' database.
        """
        if app_label in self.route_app_labels:
            return db == 'aurora_write'
        # if app_label in self.mongo_app_labels:
        #     return db == 'mongo'
        return db == 'default'

    def allow_syncdb(self, db, model):
        "Explicitly put all models on all databases."

        if model._meta.app_label in self.route_app_labels:
            return True
