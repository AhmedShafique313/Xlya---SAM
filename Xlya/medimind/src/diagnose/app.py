import os
import re
import io
import json
import time
import boto3
import pdfplumber
from botocore.exceptions import ClientError

# AWS Clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# Environment Variables
MEDIMIND_TABLE = os.environ["MEDIMIND_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET_NAME"]

# Bedrock Model — gpt-oss-120b v1 (on-demand, us-east-1, 128K context)
MODEL_ID = "openai.gpt-oss-120b-1:0"

# Detectable conditions list for prompt context
DETECTABLE_CONDITIONS = """
- Diabetes (Type 1, Type 2) & Prediabetes
- Anemia (Iron-deficiency, Megaloblastic, Hemolytic, Aplastic, Sickle Cell)
- Thyroid conditions (Hypothyroidism, Hyperthyroidism, Hashimoto's, Graves')
- Kidney disease (Chronic Kidney Disease, Acute Kidney Injury, Nephrotic Syndrome)
- Liver disease (Fatty Liver, Hepatitis, Cirrhosis, Liver Failure)
- Cardiovascular conditions (Coronary Artery Disease, Heart Failure, Hypertension, Dyslipidemia)
- Vitamin & Mineral deficiencies (Vitamin D, B12, Iron, Folate, Calcium, Magnesium)
- Blood disorders (Polycythemia, Thrombocytopenia, Leukopenia, Leukocytosis)
- Infections (Bacterial, Viral, Fungal — indicated by CBC markers)
- Autoimmune conditions (Lupus, Rheumatoid Arthritis — via inflammatory markers)
- Metabolic Syndrome & Obesity-related conditions
- Electrolyte imbalances (Hyponatremia, Hyperkalemia, etc.)
- Hormonal disorders (PCOS, Adrenal insufficiency, Cushing's syndrome)
- Nutritional deficiencies & Malabsorption syndromes
- Coagulation disorders (DVT risk, clotting factor abnormalities)
"""


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
# Extract text from PDF using pdfplumber
# ============================================================
def extract_text_from_pdf(bucket, key):
    print(f"[DIAGNOSE] Downloading PDF from s3://{bucket}/{key}")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    pdf_bytes = response["Body"].read()
    print(f"[DIAGNOSE] PDF downloaded ({len(pdf_bytes)} bytes)")

    text_lines = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            page_text = page.extract_text()
            if page_text:
                text_lines.append(page_text)
                print(f"[DIAGNOSE] Page {page_num + 1}: extracted {len(page_text)} chars")

    extracted_text = "\n".join(text_lines)
    print(f"[DIAGNOSE] Total extracted: {len(extracted_text)} chars")
    return extracted_text


# ============================================================
# Call Bedrock with retry + fallback
# ============================================================
def call_bedrock(prompt, max_retries=3):
    # GPT-style request format (no anthropic_version)
    request_body = {
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 2000,
        "temperature": 0.2,
    }

    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.invoke_model(
                modelId=MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body),
            )
            response_body = json.loads(response["body"].read().decode("utf-8"))

            # GPT-style response: choices[0].message.content
            choices = response_body.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "")

        except Exception as e:
            if "ThrottlingException" in str(e) and attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"[DIAGNOSE] Throttled, retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"[DIAGNOSE] Bedrock call failed: {e}")
                raise

    raise Exception("Bedrock model exhausted all retries")


# ============================================================
# Parse JSON from LLM response
# ============================================================
def parse_json(text):
    text = text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
    return {}


# ============================================================
# Analyze medical report with Bedrock
# ============================================================
def analyze_medical_report(report_text):
    prompt = f"""You are an expert medical AI specialized in analyzing laboratory test reports and medical documents.

You can detect the following conditions from blood work, urine tests, imaging reports, and general medical documents:
{DETECTABLE_CONDITIONS}

Below is the extracted text from a patient's medical report:

--- REPORT START ---
{report_text[:6000]}
--- REPORT END ---

Analyze this report and return your findings in the following strict JSON format. Do not include any text outside the JSON.

{{
  "primary_disease": {{
    "name": "Disease name here",
    "confidence": 87,
    "confidence_label": "87% confident"
  }},
  "secondary_disease": {{
    "name": "Second most likely disease here",
    "confidence": 63,
    "confidence_label": "63% confident"
  }},
  "reasons": [
    "Specific marker or finding from the report that supports this diagnosis",
    "Another specific marker or value that indicates this condition",
    "Any abnormal range or pattern detected"
  ],
  "details": {{
    "description": "A clear explanation of what this disease is and how it affects the body",
    "affected_organs": ["organ1", "organ2"],
    "severity": "mild | moderate | severe",
    "common_symptoms": ["symptom1", "symptom2", "symptom3"],
    "risk_factors": ["risk1", "risk2"]
  }},
  "safety_measures": [
    "Immediate action the patient should take",
    "Dietary or lifestyle recommendation",
    "Follow-up test or specialist to consult",
    "Medication or supplement guidance if applicable",
    "Warning signs to watch for"
  ],
  "disclaimer": "This analysis is AI-generated and for informational purposes only. Always consult a licensed healthcare professional for diagnosis and treatment."
}}

Only return the JSON. Be accurate, specific, and reference actual values from the report where possible."""

    print("[DIAGNOSE] Calling Bedrock for medical analysis...")
    raw_response = call_bedrock(prompt)
    print("[DIAGNOSE] Bedrock response received")
    return parse_json(raw_response)


# ============================================================
# Lambda Handler — triggered by S3 PUT event
# ============================================================
def lambda_handler(event, context):
    print(f"[DIAGNOSE] Event received: {json.dumps(event)}")

    # ---------------------------
    # Parse S3 Event
    # ---------------------------
    try:
        record = event["Records"][0]
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, IndexError) as e:
        print(f"[DIAGNOSE] Invalid S3 event: {e}")
        return cors_response(400, {"message": "Invalid S3 event structure"})

    print(f"[DIAGNOSE] Processing file: s3://{bucket}/{key}")

    # ---------------------------
    # Parse cognito_sub and report_id from S3 key
    # Key format: medimind/reports/<cognito_sub>/<report_id>_<uploaded_at>.pdf
    # ---------------------------
    try:
        parts = key.split("/")
        cognito_sub = parts[2]
        filename = parts[3]
        report_id = filename.split("_")[0]
    except (IndexError, ValueError) as e:
        print(f"[DIAGNOSE] Could not parse S3 key {key}: {e}")
        return cors_response(400, {"message": f"Unexpected S3 key format: {key}"})

    print(f"[DIAGNOSE] cognito_sub={cognito_sub}, report_id={report_id}")

    # ---------------------------
    # Update DynamoDB status → processing
    # ---------------------------
    table = dynamodb.Table(MEDIMIND_TABLE)

    try:
        table.update_item(
            Key={"report_id": report_id, "cognito_sub": cognito_sub},
            UpdateExpression="SET #st = :s",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "processing"},
        )
    except ClientError as e:
        print(f"[DIAGNOSE] DynamoDB status update failed: {e}")

    # ---------------------------
    # Extract Text from PDF via Textract
    # ---------------------------
    try:
        report_text = extract_text_from_pdf(bucket, key)
    except Exception as e:
        print(f"[DIAGNOSE] Textract failed: {e}")
        table.update_item(
            Key={"report_id": report_id, "cognito_sub": cognito_sub},
            UpdateExpression="SET #st = :s, error_message = :e",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "failed", ":e": str(e)},
        )
        return cors_response(500, {"message": f"Text extraction failed: {str(e)}"})

    if not report_text.strip():
        table.update_item(
            Key={"report_id": report_id, "cognito_sub": cognito_sub},
            UpdateExpression="SET #st = :s, error_message = :e",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "failed", ":e": "No text extracted from PDF"},
        )
        return cors_response(400, {"message": "No readable text found in the uploaded PDF"})

    # ---------------------------
    # Analyze with Bedrock
    # ---------------------------
    try:
        diagnosis = analyze_medical_report(report_text)
    except Exception as e:
        print(f"[DIAGNOSE] Bedrock analysis failed: {e}")
        table.update_item(
            Key={"report_id": report_id, "cognito_sub": cognito_sub},
            UpdateExpression="SET #st = :s, error_message = :e",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "failed", ":e": str(e)},
        )
        return cors_response(500, {"message": f"Medical analysis failed: {str(e)}"})

    # ---------------------------
    # Save Diagnosis to DynamoDB
    # ---------------------------
    diagnosed_at = str(int(time.time()))

    try:
        table.update_item(
            Key={"report_id": report_id, "cognito_sub": cognito_sub},
            UpdateExpression="""
                SET #st = :s,
                    diagnosed_at = :da,
                    primary_disease = :pd,
                    secondary_disease = :sd,
                    reasons = :r,
                    details = :det,
                    safety_measures = :sm,
                    disclaimer = :disc
            """,
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":s": "completed",
                ":da": diagnosed_at,
                ":pd": diagnosis.get("primary_disease", {}),
                ":sd": diagnosis.get("secondary_disease", {}),
                ":r": diagnosis.get("reasons", []),
                ":det": diagnosis.get("details", {}),
                ":sm": diagnosis.get("safety_measures", []),
                ":disc": diagnosis.get("disclaimer", ""),
            },
        )
        print(f"[DIAGNOSE] Diagnosis saved to DynamoDB for report_id={report_id}")
    except ClientError as e:
        print(f"[DIAGNOSE] DynamoDB save failed: {e}")
        return cors_response(500, {"message": f"Failed to save diagnosis: {str(e)}"})

    # ---------------------------
    # Success Response
    # ---------------------------
    return cors_response(
        200,
        {
            "report_id": report_id,
            "cognito_sub": cognito_sub,
            "diagnosed_at": diagnosed_at,
            "status": "completed",
            "diagnosis": {
                "primary_disease": diagnosis.get("primary_disease", {}),
                "secondary_disease": diagnosis.get("secondary_disease", {}),
                "reasons": diagnosis.get("reasons", []),
                "details": diagnosis.get("details", {}),
                "safety_measures": diagnosis.get("safety_measures", []),
                "disclaimer": diagnosis.get("disclaimer", ""),
            },
        },
    )
