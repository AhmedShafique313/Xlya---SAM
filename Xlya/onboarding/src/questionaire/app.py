import boto3
import os
import json

dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table(os.environ['USERS_TABLE'])
onboarding_table = dynamodb.Table(os.environ['ONBOARDING_TABLE'])

QUESTIONS = {
    "individual": {
        "q2": "What best describes your role?",
        "q3": "What do you want to achieve with Xlya?",
        "q4": "Which industry do you work in?"
    },
    "team": {
        "q2": "What best describes your role?",
        "q3": "What do you want to achieve with Xlya?",
        "q4": "Which industry do you work in?"
    }
}

ONBOARDING_QUESTION = "How will you use Xlya?"

def lambda_handler(event, context):
    cognito_sub = event.get("headers", {}).get("Authorization")
    if not cognito_sub:
        return response(401, {"message": "Missing Authorization header"})

    # Get user from database
    try:
        user_response = users_table.get_item(Key={"cognito_sub": cognito_sub})
        if "Item" not in user_response:
            return response(404, {"message": "User not found"})
        
        user = user_response["Item"]
    except Exception as e:
        return response(500, {"message": f"Error fetching user: {str(e)}"})

    # Parse body
    body = {}
    if event.get("body"):
        try:
            body = json.loads(event.get("body"))
        except:
            body = {}
    
    # Check onboarding status
    onboarding_status = user.get("onboarding_status", False)
    
    if onboarding_status:
        # New user - must answer the mandatory onboarding question
        answer = body.get("answer")
        
        if not answer or answer == "":
            # Return the onboarding question
            return response(200, {
                "question_id": "q1",
                "question": ONBOARDING_QUESTION,
                "onboarding_required": True
            })
        
        # Validate answer (must be "individual" or "team")
        user_type = answer.lower()
        if user_type not in ["individual", "team"]:
            return response(400, {"message": "Answer must be 'individual' or 'team'"})
        
        # Update user table with user_type and set onboarding_status to false
        try:
            users_table.update_item(
                Key={"cognito_sub": cognito_sub},
                UpdateExpression="SET user_type = :ut, onboarding_status = :os",
                ExpressionAttributeValues={
                    ":ut": user_type,
                    ":os": False
                }
            )
        except Exception as e:
            return response(500, {"message": f"Error updating user: {str(e)}"})
        
        # Save Q1 to onboarding table
        try:
            onboarding_table.update_item(
                Key={
                    "cognito_sub": cognito_sub,
                    "user_type": user_type
                },
                UpdateExpression="SET q1 = :qa",
                ExpressionAttributeValues={
                    ":qa": {
                        "question": ONBOARDING_QUESTION,
                        "answer": answer
                    }
                }
            )
        except Exception as e:
            return response(500, {"message": f"Error saving to onboarding table: {str(e)}"})
        
        return response(200, {
            "cognito_sub": cognito_sub,
            "user_type": user_type,
            "question_id": "q1",
            "question": ONBOARDING_QUESTION,
            "answer": answer,
            "onboarding_completed": True
        })
    
    else:
        # Onboarding status is false - handle subsequent questions or return nothing
        question_id = body.get("question_id")
        answer = body.get("answer")
        
        # If no question_id provided, just return success (no question to pop)
        if not question_id:
            return response(200, {
                "message": "Onboarding already completed",
                "onboarding_required": False
            })
        
        # Get user_type from database
        user_type = user.get("user_type")
        if not user_type:
            return response(400, {"message": "User type not set. Complete onboarding first."})
        
        if question_id not in QUESTIONS[user_type]:
            return response(400, {"message": "Invalid question_id"})
        
        if not answer:
            return response(400, {"message": "Answer is required"})
        
        question_text = QUESTIONS[user_type][question_id]
        
        # Save to onboarding table
        try:
            onboarding_table.update_item(
                Key={
                    "cognito_sub": cognito_sub,
                    "user_type": user_type
                },
                UpdateExpression=f"SET {question_id} = :qa",
                ExpressionAttributeValues={
                    ":qa": {
                        "question": question_text,
                        "answer": answer
                    }
                }
            )
        except Exception as e:
            return response(500, {"message": f"Error updating onboarding table: {str(e)}"})
        
        return response(200, {
            "cognito_sub": cognito_sub,
            "user_type": user_type,
            "question_id": question_id,
            "question": question_text,
            "answer": answer
        })

def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }