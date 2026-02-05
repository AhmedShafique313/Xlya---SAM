import json
import boto3
import base64
from decimal import Decimal
import os

# ---------------------------
# DynamoDB Setup
# ---------------------------
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['USERS_TABLE'])  # Set this env variable in Lambda

# ---------------------------
# Editable/Return Fields
# ---------------------------
profile_fields = [
    "first_name", "last_name", "email", "image",
    "dateofbirth", "gender", "country", "address",
    "phone_number", "age", "social_links"
]

def lambda_handler(event, context):
    # ---------------------------
    # Handle CORS Preflight
    # ---------------------------
    if event.get("httpMethod") == "OPTIONS":
        return cors_response(200, {"message": "CORS preflight success"})

    # ---------------------------
    # Get Cognito Sub from Authorization Header
    # ---------------------------
    headers = event.get("headers") or {}
    auth_header = headers.get("Authorization") or headers.get("authorization")
    if not auth_header:
        return cors_response(401, {"message": "Unauthorized: Missing Authorization header"})

    # Use the raw value from header directly (no strict Bearer check)
    if auth_header.lower().startswith("bearer "):
        cognito_sub = auth_header.split(" ", 1)[1]
    else:
        cognito_sub = auth_header

    # ---------------------------
    # Fetch user from DynamoDB
    # ---------------------------
    try:
        response = table.get_item(Key={"cognito_sub": cognito_sub})
        user = response.get("Item")
        if not user:
            return cors_response(404, {"message": "User not found"})
    except Exception as e:
        return cors_response(500, {"message": "Failed to fetch user", "error": str(e)})

    # ---------------------------
    # Prepare response body
    # ---------------------------
    result = {}
    for field in profile_fields:
        value = user.get(field)
        if value is not None:
            # Convert image to base64 if not already
            if field == "image" and not is_valid_base64(str(value)):
                try:
                    value = base64.b64encode(value.encode() if isinstance(value, str) else value).decode()
                except Exception:
                    value = None
            result[field] = value

    return cors_response(200, result)


# ---------------------------
# Base64 Validator
# ---------------------------
def is_valid_base64(data):
    try:
        if data.startswith("data:image"):
            data = data.split(",")[1]
        base64.b64decode(data, validate=True)
        return True
    except Exception:
        return False


# ---------------------------
# CORS Response Helper
# ---------------------------
def cors_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps(body, default=decimal_serializer)
    }


# ---------------------------
# Decimal Serializer
# ---------------------------
def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError
