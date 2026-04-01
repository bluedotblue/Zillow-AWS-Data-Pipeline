import boto3
import json
import pandas as pd 

s3_client = boto3.client('s3')

#event input data, context output lambda context object
def lambda_handler(event, context):

    #getting second s3 bucket name where copied json landed from first lambda function, okeeffe-zillow-project-s3-copy-json
    source_bucket = event['Records'][0]['s3']['bucket']['name'] 
    #getting filename of copied json from second s3 bucket
    object_key = event['Records'][0]['s3']['object']['key']
    #Lambda automatically has bundled permission to send logs to Cloudwatch otherwise wouldn't know how Lambda is functioning
    print(source_bucket)
    print(object_key)
    #third final s3 bucket where below tranformed json will go in pipeline
    target_bucket = 'okeeffe-zillow-project-s3-transformed-json'
    #take off .json file extension from filename string
    target_file_name = object_key[:-5]
    print(target_file_name)

    #still a little iffy on this aws waiter object, but waiter checks if above json is in above s3
    #creation
    waiter = s3_client.get_waiter('object_exists')
    #waiting
    waiter.wait(Bucket=source_bucket, Key=object_key)
    #getting the json
    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response)
    #I don't know if 'Body' is the standard key name here for all jsons, I think an boto3 object memory address is returned here
    data = response['Body']
    print(data)
    #read that binary and decode into readable characters json
    data = response['Body'].read().decode('utf-8')
    print(data)
    #loads json into python object
    jsonload = json.loads(data)
    print(jsonload)
    #how I handled my different Zillow API response to do my data wrangling 
    properties = []
    #Google Gemini gave a critique that the chained gets below might crash if 'hdpData' and 'homeInfo' are somehow missing from the payload.
    #But I didn't really know how to just clairvoyantly program/know what fields to specifically look for WITHOUT examining the actual json throughly?
    #And they seem to occur with every property listing. So, yeah. I feel like this gets the job done for now. 
    for page, responseInfo in jsonload.items():
        #get the 40 properties per page
        for property in responseInfo.get('data',[]):
            propertyInfo = property.get('hdpData', {}).get('homeInfo')
            if propertyInfo:
                properties.append(propertyInfo)
    
    print(properties)
    #turn properties list into pandas dataframe
    df = pd.DataFrame(properties)
    print(df)
    #I included some other attributes like unique property id and street address below     
    selected_columns = ['zpid', 'bedrooms', 'bathrooms', 'city', 'homeStatus', 'homeType', 'livingArea', 'price', 'rentZestimate', 'streetAddress', 'zipcode']
    df = df[selected_columns]
    print(df)
    csv_data = df.to_csv(index=False)

    #Upload csv to s3
    bucket_name = target_bucket
    object_key = f"{target_file_name}.csv"
    s3_client.put_object(Bucket = bucket_name, Key = object_key, Body = csv_data)

    return {
        'statusCode': 200,
        'body': json.dumps('CSV conversion and S3 upload completed successfully')
    }