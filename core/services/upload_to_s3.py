import time
import boto3
import pandas as pd

def s3_uploader(content, dir_name, bucket_name=None, filename=None):
    s3 = boto3.client('s3', aws_access_key_id='AKIA2XEOTUNG3F3FQ2NW' ,aws_secret_access_key='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', region_name='ap-east-1')
    content = content.encode('utf-8')

    if dir_name == None:
        return False
    
    if bucket_name == None:
        bucket_name = "droid-v2-logs"
    
    if filename ==None:
        file_name = pd.Timestamp.now()
        epoch = time.mktime(file_name.timetuple())
        s3_file = str(int(epoch)) + ".txt"
    else:
        s3_file = filename

    key = dir_name+"/"+s3_file
    upload = s3.put_object(Body=content, Bucket=bucket_name, Key=key)
    return upload