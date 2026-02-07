import boto3
import os
from datetime import datetime
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['USERS_TABLE'])

def lambda_handler(event, context):
    user_attrs = event['request']['userAttributes']
    item = {
        'cognito_sub': user_attrs['sub'],
        'email': user_attrs['email'],
        'first_name': user_attrs.get('custom:first_name', ''),
        'last_name': user_attrs.get('custom:last_name', ''),
        'user_status': 'ACTIVE',
        'onboarding_status': True,
        'coins': 50,
        'created_at': datetime.utcnow().isoformat()
    }
    table.put_item(Item=item)
    logger.info(f"User inserted into DynamoDB: {user_attrs['sub']}")
    return event