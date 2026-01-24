import boto3
import os
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['USERS_TABLE'])

def handler(event, context):
    user_attrs = event['request']['userAttributes']
    item = {
        'cognito_sub': user_attrs['sub'],
        'email': user_attrs['email'],
        'first_name': user_attrs.get('custom:first_name', ''),
        'last_name': user_attrs.get('custom:last_name', ''),
        'user_status': 'ACTIVE',
        'created_at': datetime.utcnow().isoformat()
    }
    table.put_item(Item=item)
    return event