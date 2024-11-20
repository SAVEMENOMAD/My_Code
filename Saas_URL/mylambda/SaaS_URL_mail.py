import csv
import requests
import logging
import json
import yaml
import os
import boto3
from jinja2 import Template
from io import StringIO

# Set up logging configuration
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.DEBUG)

API_KEY = '628616xMlVKlnT10'  # Make sure this API key is securely managed
# API_KEY = os.environ.get('API_KEY')

def make_api_request(tenantid, API_KEY):
    url = f"https://api.cloudplatform.accenture.com/registry/subscription/v1/tenants/{tenantid}/subscriptions"
    headers = {
        'Authorization': f"API {API_KEY}",
        'Content-Type': 'application/json'
    }
    try:
        response = requests.get(url, headers=headers)
        logging.info(f"Checking tenant ID {tenantid} against API. Response status code: {response.status_code}")
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx status codes)

        if response.status_code == 200:
            json_data = response.json()  # Convert response to JSON
            return json_data
        else:
            logging.error(f"Failed to fetch data for tenant ID {tenantid}. Status code: {response.status_code}")
            return None

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred during API request for tenant ID {tenantid}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred during API request for tenant ID {tenantid}: {req_err}")
    except Exception as e:
        logging.error(f"Exception occurred during API request for tenant ID {tenantid}: {str(e)}")

    return None

def extract_links(json_data):
    result = {}
    for item in json_data.get('data', []):
        item_id = item.get('id')
        item_state = item.get('state')
        item_config = item.get('config', {})

        if item_state == 'Subscribed':
            azure_app_link = item_config.get('azure_config', {}).get('azure_app_link')

            if item_id == 'cloudhealth':
                if azure_app_link:
                    result['cloudhealth'] = azure_app_link

            elif item_id == 'cloudeasier':
                if azure_app_link:
                    result['cloudeasier'] = azure_app_link

            elif 'prisma' in item_id:
                if azure_app_link:
                    result[item_id] = azure_app_link

            elif item_id == 'signalfx':
                if azure_app_link:
                    result['signalfx'] = azure_app_link

    return result

def load_config_file(file_name):
    try:
        with open(file_name, 'r', encoding="utf8") as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        logging.error(f"Error loading YAML file {file_name}: {e}")
    except FileNotFoundError:
        logging.error(f"Configuration file {file_name} not found.")
    except Exception as e:
        logging.error(f"Unexpected error while loading YAML file {file_name}: {e}")
    return {}

def send_notification(to_email, subject, body):
    url = 'https://api.cloudplatform.accenture.com/action/email/send'  # Update to your actual URL
    headers = {
        'Authorization': f'API {API_KEY}',
        'Content-Type': 'application/json'
    }
    emails = to_email.split(", ")
    data = {
        'subject': subject,
        'to': emails,
        'body': body,
        'title': subject
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        if response.status_code != 201:
            raise Exception(f'Error: {response.status_code} - {response.text}')
        logging.info(f"Notification sent to {to_email} with status code {response.status_code}")
        return 'Success'
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to send notification: {str(e)}')
        return f'Failed: {str(e)}'

def lambda_handler(event, context):

    S3_BUCKET_NAME = 'saas-url-mail-test'
    part_file = event.get('fileName')

    if part_file is None:
        logging.error('No part number specified in event.')
        return {
            'statusCode': 400,
            'body': 'No part number specified in event.'
        }

    logging.info(f'Starting processing for file: {part_file}')

    try:
        # Download part file from S3
        current_region = os.environ.get('AWS_REGION', 'us-east-1')
        s3_client = boto3.client('s3', region_name=current_region)
        logging.info(f'Downloading file {part_file} from S3 bucket {S3_BUCKET_NAME}')
        obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=part_file)
        data = obj['Body'].read().decode('utf-8').splitlines()

        # Load the notification configuration file
        config = load_config_file('notification.yaml').get('notification', {})
        subject = config.get('subject', "CMO SaaS Application URL")
        body_template = config.get('body', '')

        # Template for email body
        template = Template(body_template)

        # List to store result for CSV
        csv_output = []

        logging.info(f"Processing {len(data)} records.")
        for row in csv.DictReader(data):
            tenantid = row.get('tenantid')
            email = row.get('email')  # 'email' column for the recipient
            ClientName = row.get('ClientName')
            status = 'Unknown'
            
            if tenantid and email:
                logging.info(f"Processing tenant ID {tenantid}")
                json_data = make_api_request(tenantid, API_KEY)
                
                if json_data:
                    links = extract_links(json_data)
                    if links:
                        body = template.render(tenantid=tenantid, ClientName=ClientName, links=links)
                        status = send_notification(email, subject, body)
                    else:
                        status = "No links found"
                        logging.warning(f"No links found for tenant ID {tenantid}.")
                else:
                    status = "No data returned from API"
                    logging.error(f"No data returned for tenant ID {tenantid}.")
            else:
                status = "Missing tenantid or email"
                logging.warning(f"Missing tenantid or email in row: {row}")
            
            # Add the row data with status to the CSV output
            csv_output.append({
                'tenantid': tenantid,
                'email': email,
                'ClientName': ClientName,
                'status': status
            })

        # Create CSV in memory
        output_csv = StringIO()
        fieldnames = ['tenantid', 'email', 'ClientName', 'status']
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_output)

        # Upload CSV to S3
        output_csv.seek(0)
        s3_client.put_object(Body=output_csv.getvalue(), Bucket=S3_BUCKET_NAME, Key='output.csv')
        logging.info('Result CSV uploaded to S3.')

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise e

    logging.info(f'Processing of part {part_file} complete.')
    return {
        'statusCode': 200,
        'body': f'Processing of part {part_file} complete.'
    }

