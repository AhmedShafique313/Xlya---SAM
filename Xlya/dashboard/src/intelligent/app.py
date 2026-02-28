import os
import datetime
import requests
import boto3
from googleapiclient.discovery import build
from groq import Groq

# ==============================
# LOAD ENV VARIABLES (Lambda)
# ==============================
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GCP_API_KEY = os.getenv("GCP_API_KEY")
USERS_TABLE = os.getenv("USERS_TABLE")

# ==============================
# DYNAMODB SETUP
# ==============================
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(USERS_TABLE)

# ==============================
# COUNTRY MAPPINGS
# ==============================

# Country code → Google public holiday calendar
COUNTRY_HOLIDAY_CALENDAR = {
    "US": "en.usa#holiday@group.v.calendar.google.com",
    "PK": "en.pk#holiday@group.v.calendar.google.com",
    "IN": "en.indian#holiday@group.v.calendar.google.com",
    "GB": "en.uk#holiday@group.v.calendar.google.com",
}

# Country name (DB format) → ISO Code
COUNTRY_NAME_TO_CODE = {
    "pakistan": "PK",
    "united states": "US",
    "india": "IN",
    "united kingdom": "GB",
}


# ==============================
# GET LOCAL DATETIME
# ==============================
def get_local_datetime():
    return datetime.datetime.now().astimezone()


# ==============================
# GOOGLE CALENDAR CONTEXT
# ==============================
def get_today_holidays(user_country):
    calendar_id = COUNTRY_HOLIDAY_CALENDAR.get(
        user_country, COUNTRY_HOLIDAY_CALENDAR["US"]
    )

    try:
        service = build("calendar", "v3", developerKey=GCP_API_KEY)
        now = get_local_datetime()

        start_of_day_local = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day_local = now.replace(hour=23, minute=59, second=59, microsecond=999999)

        start_utc = start_of_day_local.astimezone(datetime.timezone.utc).isoformat()
        end_utc = end_of_day_local.astimezone(datetime.timezone.utc).isoformat()

        events_result = service.events().list(
            calendarId=calendar_id,
            timeMin=start_utc,
            timeMax=end_utc,
            singleEvents=True,
            orderBy="startTime",
        ).execute()

        events = events_result.get("items", [])
        return [event["summary"] for event in events]

    except Exception as e:
        print(f"[Google Calendar Error]: {e}")
        return []


# ==============================
# GROQ LLM ENGINE
# ==============================
def generate_intelligent_greeting(holidays):
    try:
        client = Groq(api_key=GROQ_API_KEY)

        now = get_local_datetime()
        hour = now.hour

        if 5 <= hour <= 11:
            time_prefix = "Morning"
        elif 12 <= hour <= 16:
            time_prefix = "Afternoon"
        elif 17 <= hour <= 20:
            time_prefix = "Evening"
        else:
            time_prefix = "Night"

        if holidays:
            holiday_str = ", ".join(holidays)
            user_prompt = (
                f"Event today: {holiday_str}. "
                f"Write ONE short, single-line greeting. "
                f"Human tone. Light rhythm if natural. "
                f"No questions. No explanations. "
                f"Start with '{time_prefix}' if natural. "
                f"Between 6 and 12 words only."
            )
        else:
            user_prompt = (
                f"Write ONE short, single-line {time_prefix.lower()} greeting. "
                f"Human tone. Smooth and natural. "
                f"No questions. No explanations. "
                f"Between 6 and 12 words only."
            )

        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are Xlya. "
                        "Respond in exactly one short line. "
                        "Never add follow-up sentences."
                    ),
                },
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.75,
            max_completion_tokens=40,
            top_p=0.9,
        )

        response = completion.choices[0].message.content.strip()
        return response.split("\n")[0]

    except Exception as e:
        print(f"[Groq API Error]: {e}")
        return "Have a wonderful day ahead."


# ==============================
# CORS RESPONSE
# ==============================
def cors_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": body,
    }


# ==============================
# AWS LAMBDA HANDLER
# ==============================
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
        return cors_response(
            401, {"message": "Unauthorized: Missing Authorization header"}
        )

    if auth_header.lower().startswith("bearer "):
        cognito_sub = auth_header.split(" ", 1)[1]
    else:
        cognito_sub = auth_header

    # ---------------------------
    # Fetch User from DynamoDB
    # ---------------------------
    try:
        response = table.get_item(Key={"cognito_sub": cognito_sub})
        user = response.get("Item")

        if not user:
            return cors_response(404, {"message": "User not found"})

        # Convert country name → ISO code
        country_name = user.get("country", "united states").lower()
        user_country = COUNTRY_NAME_TO_CODE.get(country_name, "US")

        first_name = user.get("first_name", "User")
        first_name = first_name.strip().title()

    except Exception as e:
        return cors_response(500, {"message": f"DynamoDB Error: {str(e)}"})

    # ---------------------------
    # Continue Existing Flow
    # ---------------------------
    holidays = get_today_holidays(user_country)
    greeting = generate_intelligent_greeting(holidays)

    final_message = f"{first_name}, {greeting}"

    return cors_response(200, {"message": final_message})