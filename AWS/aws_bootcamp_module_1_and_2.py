# store_id,store_name,location,sales
# 101,Store A,New York,1000
# 102,Store B,Los Angeles,1500
# 103,Store C,Chicago,900
# 104,Store D,Houston,1200



# Topics :
# 1. S3
# 2. Lambda
# 3. SNS
# 4. SQS
# 5. EventBridge


# ##########
# lambda.py
# ##########

import json

def lambda_handler(event, context):
    
    
    print("Event Data -> ", event)
    print("Trigger Received !!!")

    a = 2
    b = 3

    print("Sum of a and b = ", a+b)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Bye Bye !!')
    }
	
	
	
# ################
# S3_to_lambda.py
# ################

import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    # Get the S3 bucket and object key from the Lambda event trigger
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Use boto3 to get the CSV file from S3
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response["Body"].read().decode('utf-8')

    # Read the content using pandas
    data = pd.read_csv(StringIO(file_content))
    print(data)
    
    
    
    
    
# ##################
# SNS_from_Lambda.py
# ##################

import boto3
import pandas as pd


s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:ap-south-1:566373416292:sns-topic'

def lambda_handler(event, context):
    # TODO implement
    print(event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print(bucket_name)
        print(s3_file_key)
        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print(resp['Body'])
        df_s3_data = pd.read_csv(resp['Body'], sep=",")
        print(df_s3_data.head())
        message = "Input S3 File has been processed succesfuly !!"
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print(err)
        message = "Input S3 File processing failed !!"
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')
        
        
        
##################
##################
##     SQS      ##
##     SQS      ##
##     SQS      ##
##################
##################


# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# mock_generator_lambda.py   The purpose of this is to generate random events and to publish it for the sqs topic 
# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
'''We have mock data generator lambda that generates random data 
And we are publishing these random messages to the SQS Topic'''

import json
import boto3
import random

sqs_client = boto3.client('sqs')
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/851725469799/sqs-queue'  # replace with your SQS Queue URL

def generate_sales_order():
    return {
        "order_id": random.randint(1000, 9999),
        "product_id": random.randint(100, 999),
        "quantity": random.randint(1, 100),
        "price": round(random.uniform(10.0, 500.0), 2)
    }

def lambda_handler(event, context):
    i=0
    #To generate 100 events and publish it to sqs, we have to poll it for consumption. Since SQS is pull based, While SNS was Push based
    while(i<100):
        sales_order = generate_sales_order()
        print(sales_order)
        sqs_client.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(sales_order)
        )
        i += 1
    
    return {
        'statusCode': 200,
        'body': json.dumps('Sales order published to SQS!')
    }
    
    
    
    
# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# consumer_lambda_with_trigger.py   ---Manual polling

# '''The purpose of this is to manual poling and conume the events/message with this lambda

# EX1:    Let's create a lambda to which sqs will handover/write the message to lambda
# Here our lambda will consume/receive the message and just prints it
# After the successful delivery of the message sqs will delete the message

# Sqs_to_lambda_trigger.py'''

# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

import json

def process_sales_order(sales_order):
    print(sales_order)  # Replace with your actual processing logic.

def lambda_handler(event, context):
    # Loop through each message that triggered the lambda function
    print("Starting SQS Batch Process")
    print("Messages received in current batch = ",len(event['Records']))
    for record in event['Records']:
        sales_order = json.loads(record['body'])
        process_sales_order(sales_order)

    print("Ending SQS Batch Process")
    return {
        'statusCode': 200,
        'body': json.dumps('Processed sales orders from SQS!')
    }


# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
# consumer_lambda_without_trigger.py  -- Automated Polling

# '''EX2:    Automated Polling: sqs_to_lambda_without_trigger ( we will schedule this lambda with eventbridge).
# Here we (lambda or consumer  first poll the message and consume/process it have to delete the message 
# post consumption using the SQS Client  )

# Sqs_to_lambda_trigger.py'''

# ##################@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@

import boto3

def lambda_handler(event, context):
    print("Starting SQS Batch Process")
    # Specify your SQS queue URL
    queue_url = 'https://sqs.us-east-1.amazonaws.com/851725469799/sqs-queue'

    # Create SQS client
    sqs = boto3.client('sqs')

    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,  # Adjust based on your preference
        WaitTimeSeconds=2       # Use long polling
    )

    messages = response.get('Messages', [])  # list of 10 messages
    print("Total messages received in the batch : ",len(messages))
    for message in messages:
        # Process message
        print("Processing message: ", message['Body'])

        # Delete message from the queue
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Message deleted from the queue")
        
    print("Ending SQS Batch Process")

    return {
        'statusCode': 200,
        'body': f'{len(messages)} messages processed and deleted successfully'
    }