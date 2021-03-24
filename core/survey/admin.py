from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .forms import ForbiddenWordsForm
from .models import CensoredWords
from import_export.admin import ImportExportModelAdmin


class ForbiddenWordsAdmin(ImportExportModelAdmin):
    # add_form = CustomUserCreationForm
    form = ForbiddenWordsForm
    model = CensoredWords
    # list_display = ('email', 'is_staff', 'is_active', 'current_status')
    # list_filter = ('email', 'is_staff', 'is_active','date_joined')
    # fieldsets = (
    #     (None, {'fields': ('email', 'username',
    #                        'password', 'address', 'phone', 'first_name', 'last_name', 'country',
    #                         'birth_date', 'avatar','current_status')}),
    #     ('Permissions', {'fields': ('is_staff',
    #                                 'is_active', 'is_superuser', 'groups')}),
    # )
    # add_fieldsets = (
    #     (None, {
    #         'classes': ('wide',),
    #         'fields': ('email', 'password1', 'password2', 'is_staff', 'is_active')}
    #      ),
    # )
    # search_fields = ('email',)
    # ordering = ('email',)


admin.site.register(CensoredWords, ForbiddenWordsAdmin)

# admin.site.site_title = "services asklora"
# admin.site.site_header = "Asklora database administration"
# admin.site.index_title = "ASKLORA.AI"


