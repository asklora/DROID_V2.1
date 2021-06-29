from django.test import tag
from django.urls import reverse
from rest_framework import response,status
from rest_framework.test import APISimpleTestCase,APITestCase
from core.user.models import User

class TestAdminAccount(APITestCase):
    # databases ='__all__'
    def setUp(self):
        admin = {'email':'admin@loratechai.com',
                           'password':'admin',
                           'username':'askloraadmin',
                           'is_active':True}
        self.admin_user = User.objects.create_superuser(**admin)
        url = reverse('token_obtain_pair')
        admin.pop("username")
        admin.pop("is_active")
        resp = self.client.post(url, admin, format='json')
        self.client.credentials(HTTP_AUTHORIZATION='Bearer {0}'.format(resp.data["access"]))
        self.token = resp.data["access"]
 
        

    @tag('verify token')
    def test_verify_token(self):
        url = reverse('token_verify')
        resp = self.client.post(url, {'token':f'{self.token}'}, format='json')
        self.assertEqual(resp.status_code,status.HTTP_200_OK,msg=resp.data)
        
    @tag('verify user profile')
    def test_get_profile(self):
        url = reverse('profile')
        resp = self.client.get(url, format='json')
        self.assertEqual(resp.status_code,status.HTTP_200_OK,msg=resp.data)
        