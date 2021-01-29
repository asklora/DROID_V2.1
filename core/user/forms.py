from django.contrib.auth.forms import UserCreationForm, UserChangeForm

from .models import User


class CustomUserCreationForm(UserCreationForm):

    class Meta(UserCreationForm):
        model = User
        fields = ('email', 'phone', 'address')


class CustomUserChangeForm(UserChangeForm):

    class Meta:
        model = User
        fields = ('__all__')
