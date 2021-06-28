from django.test import TestCase,SimpleTestCase,Client
from django.urls import reverse
from rest_framework import response,status
from rest_framework.test import APISimpleTestCase,APITestCase
from core.user.models import User


class TestAccount(APITestCase):
    databases ='__all__'
    
    # def setUp(self):
    #     self.client = Client()
    
    def test_get_me(self):
        url = reverse('token_obtain_pair')
        data = {'email':'asklora@loratechai.com', 'password':'admin','username':'askloratest','is_active':True}
        user = User.objects.create_user(**data)
        print(user.password)
        data.pop('username')
        data.pop('is_active')
        print(data)
        self.assertEqual(user.is_active, True, 'Active User')
        resp = self.client.post(url, data, format='json')
        print(resp.data)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
