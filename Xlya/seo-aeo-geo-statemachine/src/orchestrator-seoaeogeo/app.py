import os
import json
import uuid
import time
import boto3
from botocore.exceptions import ClientError

# AWS Clients
dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

# Tables
USERS_TABLE = "users-table"
ANALYZER_TABLE = "seo-aeo-geo-analyzer-table"

# S3
S3_BUCKET = "xlya-bucket-dev"
S3_PREFIX = "seo-aeo-geo-analyzer"


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
    # Authorization Handling
    # ---------------------------
    headers = event.get("headers") or {}
    auth_header = headers.get("Authorization") or headers.get("authorization")

    if not auth_header:
        return cors_response(
            401, {"message": "Unauthorized: Missing Authorization header"}
        )

    if auth_header.lower().startswith("bearer "):
        cognito_sub = auth_header.split(" ", 1)[1]
    else:
        cognito_sub = auth_header

    # ---------------------------
    # Validate User from users-table
    # ---------------------------
    users_table = dynamodb.Table(USERS_TABLE)

    try:
        response = users_table.get_item(Key={"cognito_sub": cognito_sub})
        user = response.get("Item")

        if not user:
            return cors_response(404, {"message": "User not found"})

    except ClientError as e:
        return cors_response(500, {"message": str(e)})

    # ---------------------------
    # Parse Body
    # ---------------------------
    try:
        body = json.loads(event.get("body", "{}"))
    except (json.JSONDecodeError, TypeError):
        return cors_response(400, {"message": "Invalid JSON body"})

    url = str(body.get("url", "")).strip()
    brand_name = str(body.get("brand_name", "")).strip()
    keywords = str(body.get("keywords", "")).strip()
    industry = str(body.get("industry", "")).strip()

    if not all([url, brand_name, keywords, industry]):
        return cors_response(
            400,
            {"message": "Missing required fields: url, brand_name, keywords, industry"},
        )

    # ---------------------------
    # Generate Task ID
    # ---------------------------
    task_id = str(uuid.uuid4())[:8]
    created_at = str(int(time.time()))

    # ---------------------------
    # Create S3 File
    # ---------------------------
    file_content = f"""
    Task ID: {task_id}
    Cognito Sub: {cognito_sub}
    URL: {url}
    Brand Name: {brand_name}
    Keywords: {keywords}
    Industry: {industry}
    Created At: {created_at}
    """

    s3_key = f"{S3_PREFIX}/start_{task_id}_{created_at}.txt"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content.encode("utf-8"),
            ContentType="text/plain",
        )
    except ClientError as e:
        return cors_response(500, {"message": f"S3 upload failed: {str(e)}"})

    # ---------------------------
    # Store Metadata in DynamoDB
    # ---------------------------
    analyzer_table = dynamodb.Table(ANALYZER_TABLE)

    try:
        analyzer_table.put_item(
            Item={
                "task_id": task_id,
                "cognito_sub": cognito_sub,
                "created_at": created_at,
                "url": url,
                "brand_name": brand_name,
                "keywords": keywords,
                "industry": industry,
                "status": "initializing",
                "progress": "0",
                "s3_start_file": f"s3://{S3_BUCKET}/{s3_key}",
                "state_machine_path": f"s3://{S3_BUCKET}/{s3_key}",
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
            "task_id": task_id,
            "status": "initializing",
            "message": "Analysis initialization started successfully.",
            "next_step": "State machine will be triggered by S3 logic.",
        },
    )