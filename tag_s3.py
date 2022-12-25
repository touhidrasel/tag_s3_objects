#The Lambda function gets triggered by the S3 bucket object creation. 
#The Lambda function needs to read file name, s3 folder name and search for the apiKey in secret manager and tags the file with apiKey.
#Author@Touhid
#Version@1.0
#Date@06.07.2022
import os
import json
import logging
import http.client
import urllib.parse
import boto3
import base64
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
secret=''
email_body=''
tagging_value=''


print('Loading function')
s3 = boto3.client('s3')
sns_client= boto3.client('sns')

def get_timestamp():
    current = datetime.now()
    return(str(current.year) + '-' + str(current.month) + '-' + str(current.day) + '-' + str(current.hour) + '-' + str(current.minute) + '-' + str(current.second))

#gets envaironment variables        
def GetEnviron(name, default_value = ''):
    if name in os.environ:
        return os.environ[name]
    return default_value

target_topic_arn = GetEnviron('ams_fwximporter_topic_arn', 'arn:aws:sendErrorMail')
print("Env Variables : target_topic_arn " + target_topic_arn)

def lambda_handler(event, context):
    #print("Recevied event: " + json.dumps(event, indent=2))
    
    #Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    folder= os.path.dirname(key)
    print('bucket : '+bucket)
    print('key : '+key)
    secret_name = bucket+'/'+folder
    print('secret_name : '+ secret_name)
    if not get_secret_value(secret_name):
        print('Error while tagging. File: '+key)
        return
        
    print('get_secret ends..')
    try:
        response = s3.put_object_tagging(
        Bucket = bucket,
        Key = key,
        Tagging={
            'TagSet': [
                {
                    'Key': 'apiKey',
                    'Value': tagging_value
                }
            ]
        }
     )
        print('Tagging successfull..')
    except Exception as e:
        print(e)
        #print('Error applying tag {} to {}.'.format(tagName, key))
        raise e
        
def get_secret_value(secret_name):

    #secret_name = secret_name
    region_name = "us-east-1"
    print('get_secret starts..')

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
        #print('SecretId : '+ SecretId)
        )
    except Exception as e:
        global email_body
        email_body=e
        send_sns(secret_name)
        return False
        
    apiKeySecret = json.loads(get_secret_value_response['SecretString'])
    #print('Searching Secrets')
    #print(apiKeySecret['API-Key'])
    #print(apiKeySecret['instance'])
    global tagging_value
    tagging_value = apiKeySecret['apiKey']
    print('apiKey to tag :'+tagging_value)
    return True
    
    
#send sns using sns topic arn  
def send_sns(secret_name):
    sns_client.publish(
        TopicArn= target_topic_arn,
        Subject='ams-error-notification',
        Message= 'API key is not found in Secret Manager for :'+secret_name+ ' '+'Error: '+str(email_body)
    )