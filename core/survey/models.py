from django.db import models

# Create your models here.
from core.djangomodule.models import BaseTimeStampModel
from django.db.models import Value




# class LoggerSignal(BaseTimeStampModel):
#     logger_id = models.ForeignKey(ClientTopStock,on_delete=models.CASCADE,related_name="logger")
#     market_status = models.BooleanField(default=True)
#     response = models.JSONField(null=True,blank=True,default=dict)
#     status_code = models.IntegerField(null=True,blank=True)
#     logger_client_id = models.CharField(max_length=300,null=True,blank=True)
#     endpoint = models.CharField(max_length=300,null=True,blank=True)
#     header = models.JSONField(null=True,blank=True,default=dict)
#     body = models.JSONField(null=True,blank=True,default=dict)
#     event = models.CharField(max_length=250,null=True,blank=True)

#     def __str__(self):
#         return self.created
    
#     class Meta:
#         managed = True
#         db_table = "signal_logger"

# Forum DB
class Devices(BaseTimeStampModel): #(db.Model):
    device_id = models.CharField(max_length=128) #db.Column(db.String(128))
    likes_on = models.JSONField(default=dict) #db.Column(db.JSON)
    location = models.CharField(max_length=128, default=None)

    class Meta:
        managed = True
        db_table = "devices"

class ForumPost(BaseTimeStampModel): #(db.Model):
    total_like = models.IntegerField(default=0) #db.Column(db.Integer, default=0)
    content = models.TextField() #db.Column(db.Text)
    creator = models.ForeignKey(Devices,on_delete=models.CASCADE) #db.Column(db.Integer)
    # created = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    class Meta:
        managed = True
        db_table = "forum_post"

# Survey DB
class InvestorTypes(BaseTimeStampModel): #(db.Model):
    investor_category = models.CharField(max_length=128) #db.Column(db.String(128))
    
    class Meta:
        managed = True
        db_table = "investor_types"
   
class SurveyResults(BaseTimeStampModel): #(db.Model):
    email = models.CharField(max_length=128, unique=True) #db.Column(db.String(128))
    answer = models.JSONField(default=dict) #db.Column(db.JSON)
    investor_type = models.ForeignKey(InvestorTypes,on_delete=models.CASCADE) #db.Column(db.Integer)
    read_policy = models.BooleanField() #db.Column(db.Boolean)
    devices = models.ForeignKey(Devices,on_delete=models.CASCADE) #db.Column(db.Integer)
    # timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    class Meta:
        managed = True
        db_table = "survey_results"
