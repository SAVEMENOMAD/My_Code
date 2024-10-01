import boto3
import json

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # S3 bucket and file details
    input_bucket = "detaileds3"
    input_file_key = "path/to/input_file.json"  # Modify with the actual file path

    # Fetch input file from S3
    try:
        response = s3_client.get_object(Bucket=input_bucket, Key=input_file_key)
        input_file_content = response['Body'].read().decode('utf-8')
        input_data = json.loads(input_file_content)
    except Exception as e:
        print(f"Error fetching input file: {str(e)}")
        return {"status": "Error", "message": "Failed to fetch input file"}

    for bucket_detail in input_data.get("buckets", []):
        account_id = bucket_detail.get("accountId")
        if not account_id:
            continue

        s3_bucket_name = f"acp-siem-integration-{account_id}-bucket"

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=s3_bucket_name)
        except Exception:
            print(f"S3 bucket {s3_bucket_name} does not exist")
            bucket_detail['status'] = "Request"
            continue

        # Check for "AWSLogs/" object in the bucket
        awslogs_prefix = "AWSLogs/"
        if not check_s3_object_exists(s3_bucket_name, awslogs_prefix):
            bucket_detail['status'] = "Request"
            continue

        # Check for "{account_id}/" object inside "AWSLogs/"
        account_prefix = f"AWSLogs/{account_id}/"
        if not check_s3_object_exists(s3_bucket_name, account_prefix):
            bucket_detail['status'] = "Request"
            continue

        # Check for "CloudTrail/" object inside "{account_id}/"
        cloudtrail_prefix = f"{account_prefix}CloudTrail/"
        if not check_s3_object_exists(s3_bucket_name, cloudtrail_prefix):
            bucket_detail['status'] = "Request"
            continue

        # Check for "us-east-1/" object inside "CloudTrail/"
        us_east_1_prefix = f"{cloudtrail_prefix}us-east-1/"
        if not check_s3_object_exists(s3_bucket_name, us_east_1_prefix):
            bucket_detail['status'] = "Request"
            continue

        # Check for "2024/" object inside "us-east-1/"
        year_2024_prefix = f"{us_east_1_prefix}2024/"
        if not check_s3_object_exists(s3_bucket_name, year_2024_prefix):
            bucket_detail['status'] = "Request"
            continue

        # If all checks pass, update the status as appropriate
        bucket_detail['status'] = "Complete"

    # Update input file in S3
    try:
        updated_input_file_content = json.dumps(input_data, indent=2)
        s3_client.put_object(Bucket=input_bucket, Key=input_file_key, Body=updated_input_file_content)
    except Exception as e:
        print(f"Error updating input file: {str(e)}")
        return {"status": "Error", "message": "Failed to update input file"}

    return {"status": "Success", "message": "Input file updated"}

# Helper function to check if an S3 object or prefix exists
def check_s3_object_exists(bucket_name, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response
    except Exception as e:
        print(f"Error checking object: {str(e)}")
        return False

