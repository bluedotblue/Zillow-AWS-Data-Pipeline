import boto3
import json

s3_client = boto3.client('s3')

#lambda function takes in data input in the form of an "event" or trigger
#and then transforms that data into the form of a "context object"
def lambda_handler(event, context):
    
    #source bucket is the initial s3 that contains the zillow api json deposited from beginning ec2 airflow dag
    source_bucket = event['Records'][0]['s3']['bucket']['name'] #okeeffe-zillow-project-s3
    object_key = event['Records'][0]['s3']['object']['key'] #full path and filename of object uploaded to s3 ...json

    target_bucket = 'okeeffe-zillow-project-s3-copy-json' #new second s3 landing bucket for copied json to go to. okeeffe-zillow-project-s3-copy-json
    copy_source = {'Bucket': source_bucket, 'Key': object_key} #dictionary of above source attributes

    #check if above object is in s3 by creating a "waiter"
    waiter = s3_client.get_waiter('object_exists')
    #wait on above object in initial s3 bucket
    waiter.wait(Bucket=source_bucket, Key=object_key)


    #actually do the copying to the second new s3 bucket
    s3_client.copy_object(Bucket=target_bucket, Key=object_key, CopySource=copy_source)

    #I believe this goes into Cloudwatch in a log
    return {
        'statusCode': 200,
        'body': json.dumps('Copy completed successfully')
    }