Run Athena Queries using AWS Lambda Function and store the output in a S3 Bucket: 

# This can be used when we want to run some query on our data set at a regular interval and pass the output to next step in the Pipeline 

We will use the boto3 library, provide the athena database table (Sales_db here)

In the lambda handler function:

We pass all the requirements to run the query, but we also need the response and staus if the query is success or fail and the execution Id

That is being handled in the script as well 

In the while loop the state of the query is being checked continuously 
 
