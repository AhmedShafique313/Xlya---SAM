import json
import boto3
import base64
from datetime import datetime
from decimal import Decimal

# ---------------------------
# DynamoDB Setup
# ---------------------------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("users-table")


def lambda_handler(event, context):
    # ---------------------------
    # Handle CORS Preflight
    # ---------------------------
    if event.get("httpMethod") == "OPTIONS":
        return cors_response(200, {"message": "CORS preflight success"})

    # ---------------------------
    # Get Cognito User (sub)
    # ---------------------------
    try:
        cognito_sub = event["requestContext"]["authorizer"]["claims"]["sub"]
    except KeyError:
        return cors_response(
            401,
            {"message": "Unauthorized: Cognito user not found"}
        )

    # ---------------------------
    # Parse Body
    # ---------------------------
    body = event.get("body") or "{}"
    if isinstance(body, str):
        body = json.loads(body)

    # ---------------------------
    # Allowed Editable Fields
    # ---------------------------
    editable_fields = [
        "first_name",
        "last_name",
        "email",
        "image",               # base64 string
        "dateofbirth",
        "gender",
        "country",
        "address",
        "phone_number",
        "age",
        "social_links"
    ]

    update_expression = []
    expression_names = {}
    expression_values = {}

    # ---------------------------
    # Normalize & Validate Age
    # ---------------------------
    if "age" in body:
        body["age"] = int(body["age"])

    # ---------------------------
    # Validate base64 image (if provided)
    # ---------------------------
    if "image" in body:
        image_base64 = body["image"]

        if not is_valid_base64(image_base64):
            return cors_response(
                400,
                {"message": "Invalid base64 image format"}
            )

        # Store exactly as received (string)
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

    # No valid fields check
    if len(update_expression) == 1:
        return cors_response(
            400,
            {"message": "No valid profile fields provided"}
        )

    # ---------------------------
    # DynamoDB Update
    # ---------------------------
    result = table.update_item(
        Key={"cognito_sub": cognito_sub},
        UpdateExpression="SET " + ", ".join(update_expression),
        ExpressionAttributeNames=expression_names,
        ExpressionAttributeValues=expression_values,
        ReturnValues="UPDATED_NEW"
    )

    # ---------------------------
    # Success Response
    # ---------------------------
    return cors_response(
        200,
        {
            "message": "User profile updated successfully",
            "updated_fields": result.get("Attributes", {})
        }
    )


# ---------------------------
# Base64 Validator
# ---------------------------
def is_valid_base64(data):
    try:
        # Remove data URL prefix if frontend sends it
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
