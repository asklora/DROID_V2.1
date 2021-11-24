from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .forms import CustomUserCreationForm, CustomUserChangeForm
from .models import User,TransactionHistory


from rest_framework_simplejwt import token_blacklist

class OutstandingTokenAdmin(token_blacklist.admin.OutstandingTokenAdmin):

    def has_delete_permission(self, *args, **kwargs):
        return True # or whatever logic you want

admin.site.unregister(token_blacklist.models.OutstandingToken)
admin.site.register(token_blacklist.models.OutstandingToken, OutstandingTokenAdmin)



class AppUserAdmin(UserAdmin):
    add_form = CustomUserCreationForm
    form = CustomUserChangeForm
    model = User
    list_display = ('email', 'is_staff', 'is_active', 'current_status','username','is_test')
    list_filter = ('is_staff', 'is_active','date_joined','current_status','is_test')
    fieldsets = (
        (None, {'fields': ('email', 'username',
                           'password', 'address', 'phone', 'first_name', 'last_name',
                            'birth_date', 'avatar','current_status')}),
        ('Permissions', {'fields': ('is_staff',
                                    'is_active', 'is_superuser', 'groups')}),
    )
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('email', 'password1', 'password2', 'is_staff', 'is_active')}
         ),
    )
    search_fields = ('email','username')
    ordering = ('email',)


admin.site.register(User, AppUserAdmin)
admin.site.register(TransactionHistory)
admin.site.site_title = "services asklora"
admin.site.site_header = "Asklora database administration"
admin.site.index_title = "ASKLORA.AI"


