import json
import boto3
import base64
from datetime import datetime
from decimal import Decimal
import os

# ---------------------------
# DynamoDB Setup
# ---------------------------
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['USERS_TABLE'])  # Set this env variable in Lambda


def lambda_handler(event, context):
    # ---------------------------
    # Handle CORS Preflight
    # ---------------------------
    if event.get("httpMethod") == "OPTIONS":
        return cors_response(200, {"message": "CORS preflight success"})

    # ---------------------------
    # Get Cognito Sub from Authorization Header (like onboarding Lambda)
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
    # Parse Body robustly
    # ---------------------------
    body = event.get("body") or "{}"

    # If string, parse JSON; if dict, use as-is
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return cors_response(400, {"message": "Invalid JSON body"})
    elif not isinstance(body, dict):
        return cors_response(400, {"message": "Invalid body format"})

    # ---------------------------
    # Editable Fields
    # ---------------------------
    editable_fields = [
        "first_name", "last_name", "email", "image",
        "dateofbirth", "gender", "country", "address",
        "phone_number", "age", "social_links"
    ]

    update_expression = []
    expression_names = {}
    expression_values = {}

    # ---------------------------
    # Normalize Age
    # ---------------------------
    if "age" in body:
        try:
            body["age"] = int(body["age"])
        except ValueError:
            return cors_response(400, {"message": "Age must be a number"})

    # ---------------------------
    # Validate Base64 Image
    # ---------------------------
    if "image" in body:
        image_base64 = body["image"]
        if not is_valid_base64(image_base64):
            return cors_response(400, {"message": "Invalid base64 image format"})
        body["image"] = image_base64

    # ---------------------------
    # Build Update Expression
    # ---------------------------
    for field in editable_fields:
        if field in body:
            update_expression.append(f"#{field} = :{field}")
            expression_names[f"#{field}"] = field
            expression_values[f":{field}"] = body[field]

    # Always update updated_at
    update_expression.append("#updated_at = :updated_at")
    expression_names["#updated_at"] = "updated_at"
    expression_values[":updated_at"] = datetime.utcnow().isoformat()

    if len(update_expression) == 1:
        return cors_response(400, {"message": "No valid profile fields provided"})

    # ---------------------------
    # DynamoDB Update
    # ---------------------------
    try:
        result = table.update_item(
            Key={"cognito_sub": cognito_sub},
            UpdateExpression="SET " + ", ".join(update_expression),
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values,
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        return cors_response(500, {"message": "Failed to update user profile", "error": str(e)})

    # ---------------------------
    # Success Response
    # ---------------------------
    return cors_response(200, {
        "message": "User profile updated successfully",
        "updated_fields": result.get("Attributes", {})
    })


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
            "Access-Control-Allow-Methods": "OPTIONS,POST,PUT"
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
