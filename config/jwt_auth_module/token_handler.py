from rest_framework_simplejwt.authentication import JWTAuthentication
from rest_framework_simplejwt.exceptions import InvalidToken,TokenError
from core.djangomodule.general import NeedRegister
from rest_framework_simplejwt.settings import api_settings
from django.utils.translation import gettext_lazy as _


class AuthJwt(JWTAuthentication):
    
    def get_validated_token(self, raw_token):
        """
        Validates an encoded JSON web token and returns a validated token
        wrapper object.
        """
        messages = []
        for AuthToken in api_settings.AUTH_TOKEN_CLASSES:
            try:
                return AuthToken(raw_token)
            except TokenError as e:
                messages.append({'token_class': AuthToken.__name__,
                                 'token_type': AuthToken.token_type,
                                 'message': e.args[0]})

        raise NeedRegister(detail={"detail":"Token invalid"},code=400)

    def get_user(self, validated_token):
        """
        Attempts to find and return a user using the given validated token.
        """
        try:
            user_id = validated_token[api_settings.USER_ID_CLAIM]
        except KeyError:
            raise InvalidToken(_('Token contained no recognizable user identification'))

        try:
            user = self.user_model.objects.get(**{api_settings.USER_ID_FIELD: user_id})
        except self.user_model.DoesNotExist:
            raise NeedRegister()

        if not user.is_active:
            raise NeedRegister()

        return user
    
    def get_user_from_token(self,validated_token):
        try:
            user_id = validated_token[api_settings.USER_ID_CLAIM]
        except KeyError:
            return None

        try:
            user = self.user_model.objects.get(**{api_settings.USER_ID_FIELD: user_id})
        except self.user_model.DoesNotExist:
            return None


        return user