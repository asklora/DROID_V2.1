from django.contrib.auth.base_user import BaseUserManager
from django.utils.translation import ugettext_lazy as _
import uuid
from django.db import models

class AppUserManager(BaseUserManager):

    def create_unique_username(self, email):
        strip = email.replace('@', '_').replace(
            '.com', '').replace('.', '_')
        unique_usr = "%s%s" % (uuid.uuid4().hex[:8], strip)
        return unique_usr

    def create_user(self, email=None, username=None, password=None, **extra_fields):
        if not username:
            raise ValueError(_('Users must have an username'))
        if username == '' or not username:
            user = self.model(username=self.create_unique_username(
                email), email=email, **extra_fields)
        else:
            user = self.model(username=username, email=email, **extra_fields)
        if email:
            email = self.normalize_email(email)
        user = self.model(username=username, email=email, **extra_fields)
        user.set_password(password)
        user.save()
        return user

    def create_superuser(self, email, username, password, **extra_fields):
        """
        Create and save a SuperUser with the given email and password.
        """
        if not username:
            strip = email.replace('@', '_').replace(
                '.com', '').replace('.', '_')
            unique_usr = "%s%s" % (uuid.uuid4().hex[:8], strip)
            username = unique_usr
        extra_fields.setdefault('is_staff', True)
        extra_fields.setdefault('is_superuser', True)
        extra_fields.setdefault('is_active', True)

        if extra_fields.get('is_staff') is not True:
            raise ValueError(_('Superuser must have is_staff=True.'))
        if extra_fields.get('is_superuser') is not True:
            raise ValueError(_('Superuser must have is_superuser=True.'))
        return self.create_user(email, username, password, **extra_fields)



