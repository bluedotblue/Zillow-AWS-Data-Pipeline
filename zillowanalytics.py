#Create a file named .gitignore in your project folder and add the line config_api.json to it.
#then do git versioning

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import json
import requests
import time

#load json config file
with open("/home/ubuntu/airflow/config_api.json", "r") as config_file:
    api_host_key = json.load(config_file)

right_now = datetime.now()
dt_now_string = right_now.strftime("%Y%m%d_%H%M%S")
s3_bucket = 'okeeffe-zillow-project-s3-transformed-json'

#python callable user defined function within below DAG
#altered to include more properties than only properties on first page of API
def extract_zillow_data(**kwargs):
    propertiesJson = {}
    pagesPerExtraction = 50
    url = kwargs['url']
    headers = kwargs['headers']
    dt_string = kwargs['date_string']
    
    #altered loop for 100 page extraction
    for currentPage in range(1, pagesPerExtraction + 1):
        querystring = {
            'url': kwargs['querystring']['url'],
            'page': str(currentPage)
        }
    
        #return headers
        response = requests.get(url, headers = headers, params = querystring)
        
        #if response is successful
        if response.status_code == 200:
            #get entire json response for lambda function to transform
            response_data = response.json()

            if not response_data:
                print(f"Empty page: {currentPage}")
                break

            #ATTACHING CURRENT PAGE RESPONSE TO PROPERTIES JSON HERE
            #properties.extend(response_data)
            propertiesJson[currentPage] = response_data
            print(f"Successfully fetched page: {currentPage}")

            #allow 1 second to pass for rate limit of US Property Data API
            time.sleep(1)

        else:
            print(f"Failed to fetch page: {currentPage} with status code: {response.status_code}")
            break

    #specific output file path and file string with datetime of extraction
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f"response_data_{dt_string}.csv"

    #write the json response to a file inside ec2 instance
    with open(output_file_path, "w") as output_file:
        
        json.dump(propertiesJson, output_file, indent = 4)

    output_list = [output_file_path, file_str]
    
    return output_list

#airflow parameters
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 8),
    "email": ["myemail@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds = 15)
}

#initiate DAG(Directed Acyclic Graph)
with DAG("zillow_analytics_dag",
        default_args = default_args,
        schedule_interval = "@daily", #running everyday at 12AM
        catchup = False
) as dag:

    #within DAG with statement
    extract_zillow_data_var = PythonOperator(
        task_id = "task_extract_zillow_data_var",
        python_callable = extract_zillow_data,
        op_kwargs = {
            "url": "https://us-property-data.p.rapidapi.com/api/v1/search/by-url", 
            "querystring": {
                "url":"https://www.zillow.com/houston-tx/"
            }, 
            "headers": api_host_key,
            "date_string": dt_now_string
        }
    )

    load_to_s3 = BashOperator(
        task_id = "task_load_to_s3",
        bash_command = "aws s3 mv {{ ti.xcom_pull('task_extract_zillow_data_var')[0] }} s3://okeeffe-zillow-project-s3/"
    )

    is_file_in_s3_available = S3KeySensor(
        task_id = "task_is_file_in_s3_available",
        bucket_key = '{{ti.xcom_pull("task_extract_zillow_data_var")[1]}}',
        bucket_name = s3_bucket,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        timeout = 60, #timeout for the s3 sensor
        poke_interval = 5 #time interval in between checking s3 bucket
    )

    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id = "task_transfer_s3_to_redshift",
        aws_conn_id = "aws_s3_conn",
        redshift_conn_id = "aws_redshift_conn",
        s3_bucket = s3_bucket,
        s3_key = "{{ti.xcom_pull('task_extract_zillow_data_var')[1]}}",
        schema = "PUBLIC",
        table = "zillow_initial_staging",
        copy_options = ["csv IGNOREHEADER 1"]
    )

    deduplicate_uniqueProperties_table = SQLExecuteQueryOperator(
        task_id = "task_deduplicate_uniqueProperties_table",
        conn_id = "aws_redshift_conn",
        sql = """
            --bundle delete and insert into a single transaction in case of outage
            BEGIN TRANSACTION;

            --delete matching zpids in uniqueProperties main table to resolve duplicates from zillow_initial_staging table
            DELETE FROM uniqueProperties
            USING zillow_initial_staging
            WHERE uniqueProperties.zpid = zillow_initial_staging.zpid;

            --insert into main uniqueProperties table the most recent(least expensive) price of home since timestamp was not included in my original plan
            INSERT INTO uniqueProperties (zpid, bedrooms, bathrooms, city, homeStatus, homeType, livingArea, price, rentZestimate, streetAddress, zipcode)
            SELECT zpid, bedrooms, bathrooms, city, homeStatus, homeType, livingArea, price, rentZestimate, streetAddress, zipcode
            FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY zpid ORDER BY price ASC) AS rank FROM zillow_initial_staging)
            WHERE rank = 1;

            --clear zillow_initial_staging table for next DAG
            DELETE FROM zillow_initial_staging;

            END TRANSACTION;
            """,
        split_statements = True 
    )



    #airflow tasks dependencies for DAG
    extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift >> deduplicate_uniqueProperties_table

