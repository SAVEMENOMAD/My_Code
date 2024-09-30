import json
import boto3
import csv
from io import StringIO

# Initialize boto3 clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')  # Add the Lambda client

DYNAMODB_TABLE = "fetchclientadminreport"
S3_BUCKET = "saas-url-mail-test"
SECOND_LAMBDA_FUNCTION = "SaaS_URL_mail_input_break"  # Name of the second Lambda

def lambda_handler(event, context):
    # Scan the DynamoDB table to get all items
    response = dynamodb.scan(TableName=DYNAMODB_TABLE)
    
    # List to hold tenant ID and email (id)
    data_to_store = []

    # Iterate through all items
    for item in response['Items']:
        primary_tenant_id = item.get('primaryTenantId', {}).get('N', None)
        email = item.get('id', {}).get('S', None)
        ClientName = item.get('Clientname', {}).get('S', None)
        
        # Only add items that contain both a primaryTenantId and an email
        if primary_tenant_id and email:
            data_to_store.append([primary_tenant_id, email,ClientName])

    # If there is data to store, create a CSV and upload to S3
    if data_to_store:
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)
        
        # Write the header
        csv_writer.writerow(['tenantid', 'email' , 'ClientName' ] , )
        
        # Write the data rows
        csv_writer.writerows(data_to_store)
        
        # Set the S3 object key (filename)
        s3_key = 'input_mail.csv'
        
        # Upload the CSV file to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        # Invoke the second Lambda function asynchronously
        invoke_response = lambda_client.invoke(
            FunctionName=SECOND_LAMBDA_FUNCTION,
            InvocationType='Event',  # Use 'Event' for async invocation
            Payload=json.dumps({
                'message': f'CSV {s3_key} uploaded to {S3_BUCKET}',
                's3_key': s3_key,
                'bucket': S3_BUCKET
            })
        )
        
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully uploaded {s3_key} to {S3_BUCKET} and triggered {SECOND_LAMBDA_FUNCTION}')
            
            
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps('No data found for client-admin report')
        }

