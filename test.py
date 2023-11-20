import boto3
from botocore.exceptions import ClientError

def get_secret():
    secret_name = "weather_api_secret"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

# Test the function outside the function definition
try:
    api_key = get_secret()
    print(f"Retrieved API Key: {api_key}")
except ClientError as error:
    print(f"An error occurred: {error}")
