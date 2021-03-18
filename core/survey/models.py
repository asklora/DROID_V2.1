from django.db import models

# Create your models here.
from core.djangomodule.models import BaseTimeStampModel
from django.db.models import Value


# Forum DB
class Devices(BaseTimeStampModel): #(db.Model):
	device_id = models.CharField(max_length=128) #db.Column(db.String(128))
	likes_on = models.JSONField(default=dict) #db.Column(db.JSON)
	location = models.CharField(max_length=128, default="Indonesia")

	class Meta:
		managed = True
		db_table = "survey_devices"

class ForumPost(BaseTimeStampModel): #(db.Model):
	total_like = models.IntegerField(default=0) #db.Column(db.Integer, default=0)
	content = models.TextField() #db.Column(db.Text)
	creator = models.ForeignKey(Devices,on_delete=models.CASCADE) #db.Column(db.Integer)

	class Meta:
		managed = True
		db_table = "survey_forum_post"

# Survey DB
class InvestorTypes(BaseTimeStampModel): #(db.Model):
	investor_category = models.CharField(max_length=128) #db.Column(db.String(128))
	
	class Meta:
		managed = True
		db_table = "survey_investor_types"
   
class SurveyResults(BaseTimeStampModel): #(db.Model):
	email = models.CharField(max_length=128, unique=True) #db.Column(db.String(128))
	answer = models.JSONField(default=dict) #db.Column(db.JSON)
	investor_type = models.ForeignKey(InvestorTypes,on_delete=models.CASCADE) #db.Column(db.Integer)
	read_policy = models.BooleanField() #db.Column(db.Boolean)
	devices = models.ForeignKey(Devices,on_delete=models.CASCADE, default=2) #db.Column(db.Integer)

	class Meta:
		managed = True
		db_table = "survey_results"

class CensoredWords(BaseTimeStampModel):
	country =  models.CharField(max_length=128, unique=True)
	censored_words = models.JSONField(default=dict)

	class Meta:
		managed = True
		db_table = "survey_censored_words"
