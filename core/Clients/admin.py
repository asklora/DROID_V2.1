from django.contrib import admin
from .models import Client,UniverseClient,UserClient, ClientTopStock

admin.site.register(Client)
admin.site.register(UniverseClient)
admin.site.register(UserClient)
admin.site.register(ClientTopStock)
