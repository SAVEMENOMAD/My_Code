import boto3
import csv
import logging
import os
import json

S3_BUCKET_NAME = 'saas-url-mail-test'
S3_INPUT_FILE_KEY = 'input_mail.csv'
PART_SIZE = 5000

#fucntion to validate users with graph API

PROCESSING_FUNCTION_NAME = 'SaaS_URL_mail' 


def split_input_file(event, context):
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    local_input_file = '/tmp/input_mail.csv'

    try:
        # Download input file from S3
        s3.download_file(S3_BUCKET_NAME, S3_INPUT_FILE_KEY, local_input_file)
        logging.info("Successfully downloaded input.csv from S3 bucket.")

        with open(local_input_file, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Read header row
            rows = list(reader)
        
        total_parts = (len(rows) + PART_SIZE - 1) // PART_SIZE

        for part in range(total_parts):
            part_file_name = f'input_part_{part + 1}.csv'
            local_part_file = f'/tmp/{part_file_name}'
            with open(local_part_file, 'w', newline='') as part_file:
                writer = csv.writer(part_file)
                writer.writerow(header)
                part_rows = rows[part * PART_SIZE:(part + 1) * PART_SIZE]
                writer.writerows(part_rows)
            
            s3.upload_file(local_part_file, S3_BUCKET_NAME, f'{part_file_name}')
            logging.info(f"Successfully uploaded {part_file_name} to S3 bucket.")
            
            lambda_client = boto3.client('lambda')
            
            # Invoke the processing function
            invoke_response = lambda_client.invoke(
                FunctionName=PROCESSING_FUNCTION_NAME,
                InvocationType='Event',
                #Payload=json.dumps({'part_number': part + 1})
                Payload=json.dumps({'fileName': part_file_name}))
            logging.info(f"Invoked processing function for part {part + 1}. Response: {invoke_response}")

        return {
            'statusCode': 200,
            'body': 'Splitting complete. Part files uploaded to S3 and processing functions invoked.'
        }

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise e

