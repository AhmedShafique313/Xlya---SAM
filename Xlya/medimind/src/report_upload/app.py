import os
import json
import uuid
import time
import boto3
from botocore.exceptions import ClientError

# AWS Clients
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

# Environment Variables
MEDIMIND_TABLE = os.environ["MEDIMIND_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = "medimind/reports"


# ============================================================
# CORS Response
# ============================================================
def cors_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "POST,OPTIONS",
        },
        "body": json.dumps(body),
    }


# ============================================================
# Lambda Handler
# ============================================================
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

    if auth_header.lower().startswith("bearer "):
        cognito_sub = auth_header.split(" ", 1)[1]
    else:
        cognito_sub = auth_header

    # ---------------------------
    # Fetch user from DynamoDB
    # ---------------------------
    users_table = dynamodb.Table("users-table")

    try:
        response = users_table.get_item(Key={"cognito_sub": cognito_sub})
        user = response.get("Item")
        if not user:
            return cors_response(404, {"message": "User not found"})
    except ClientError as e:
        return cors_response(500, {"message": "Failed to fetch user", "error": str(e)})

    # ---------------------------
    # Generate Report ID & Timestamp
    # ---------------------------
    report_id = str(uuid.uuid4())[:4]
    uploaded_at = str(int(time.time()))

    # ---------------------------
    # Generate Pre-Signed URL for PDF Upload
    # ---------------------------
    s3_key = f"{S3_PREFIX}/{cognito_sub}/{report_id}_{uploaded_at}.pdf"

    try:
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": S3_BUCKET,
                "Key": s3_key,
                "ContentType": "application/pdf",
            },
            ExpiresIn=300,
        )
    except ClientError as e:
        return cors_response(500, {"message": f"Failed to generate pre-signed URL: {str(e)}"})

    # ---------------------------
    # Store Record in DynamoDB
    # ---------------------------
    medimind_table = dynamodb.Table(MEDIMIND_TABLE)

    try:
        medimind_table.put_item(
            Item={
                "report_id": report_id,
                "cognito_sub": cognito_sub,
                "uploaded_at": uploaded_at,
                "s3_key": s3_key,
                "status": "pending",
            }
        )
    except ClientError as e:
        return cors_response(500, {"message": f"DynamoDB error: {str(e)}"})

    # ---------------------------
    # Success Response
    # ---------------------------
    return cors_response(
        200,
        {
            "report_id": report_id,
            "uploaded_at": uploaded_at,
            "presigned_url": presigned_url,
            "s3_key": s3_key,
            "status": "pending",
            "message": "Pre-signed URL generated. Upload your PDF to the provided URL.",
        },
    )
