# from django.contrib.auth.forms import UserCreationForm, UserChangeForm
from django.forms import forms,ModelForm

from .models import CensoredWords


class ForbiddenWordsForm(ModelForm):

    class Meta:
        model = CensoredWords
        fields = ('country', 'censored_words')


# class CustomUserChangeForm(UserChangeForm):

#     class Meta:
#         model = CensoredWords
#         fields = ('__all__')
