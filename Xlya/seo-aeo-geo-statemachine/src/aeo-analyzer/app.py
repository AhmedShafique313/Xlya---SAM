import os
import json
import re
import boto3
import time
import random
import traceback

def lambda_handler(event, context):
    """
    Input from Step Functions (output of Crawl Lambda):

    {
      "task_id": "...",
      "crawl_data": {...},
      "metadata": {
          "brand_name": "...",
          "keywords": "...",
          "industry": "..."
      }
    }
    """

    task_id = event.get("task_id")
    crawl_data = event.get("crawl_data", {})
    metadata = event.get("metadata", {})

    brand_name = metadata.get("brand_name", "")
    keywords_str = metadata.get("keywords", "")
    industry = metadata.get("industry", "")

    keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]

    print(f"[AEO] Starting AEO analysis for task {task_id}")

    aeo_result = _analyze_aeo(crawl_data, brand_name, keywords, industry)

    return {
        "task_id": task_id,
        "message": "Running AEO analysis with AI...",
        "aeo_data": aeo_result,
        "crawl_data": crawl_data,  # Pass through for next Lambdas
        "metadata": metadata        # Pass through for next Lambdas
    }


def call_bedrock_with_retry(bedrock_runtime, model_id, request_body, max_retries=5):
    """Call Bedrock with exponential backoff retry logic"""
    
    for attempt in range(max_retries):
        try:
            response = bedrock_runtime.invoke_model(
                modelId=model_id,
                contentType='application/json',
                accept='application/json',
                body=json.dumps(request_body)
            )
            return response
            
        except Exception as e:
            if "ThrottlingException" in str(e):
                if attempt == max_retries - 1:
                    print(f"[AEO] Max retries reached for throttling")
                    raise
                
                # Exponential backoff with jitter (2^attempt seconds + random jitter)
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"[AEO] Throttled. Retrying in {wait_time:.2f} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                # Not a throttling exception, re-raise immediately
                raise


def _analyze_aeo(crawl_data, brand_name, keywords, industry):
    """
    Analyze content for Answer Engine Optimization using Claude 3 on AWS Bedrock
    """
    
    # Check if we have the required AWS configuration
    try:
        # Create Bedrock runtime client - explicitly set region to us-east-1
        bedrock_runtime = boto3.client(
            'bedrock-runtime',
            region_name='us-east-1'  # Explicitly set to N. Virginia
        )
        print("[AEO] Bedrock client created successfully in us-east-1")
    except Exception as e:
        print(f"[AEO] Failed to create Bedrock client: {e}")
        print(traceback.format_exc())
        return _get_fallback_result(brand_name, crawl_data)

    # Model IDs for Claude 3 in us-east-1
    # Use Haiku for better rate limits, fallback to Sonnet if needed
    PRIMARY_MODEL = 'openai.gpt-oss-120b-1:0'
    FALLBACK_MODEL = 'openai.gpt-oss-120b-1:0'
    
    print(f"[AEO] Using primary Bedrock model: {PRIMARY_MODEL}")
    print(f"[AEO] Fallback model: {FALLBACK_MODEL}")

    factors = {}
    recommendations = []

    # REDUCE TOKEN USAGE - truncate content to avoid throttling
    content_summary = crawl_data.get("markdown", "")[:2500]  # Reduced from 5000
    title = crawl_data.get("title", "")
    description = crawl_data.get("description", "")
    headings = crawl_data.get("headings", {})

    # ---------- 1 AI Visibility ----------
    visibility_prompt = f"""You are an AI search engine evaluator.

Analyze whether this website content would be cited by AI answer engines
(ChatGPT, Google AI Overview, Perplexity).

Keywords: {', '.join(keywords)}
Industry: {industry}

Brand: {brand_name}
Title: {title}
Description: {description}

Content:
{content_summary[:2000]}  # Further reduced for this call

Return your analysis in this exact JSON format:
{{
"citation_likelihood": 0-100,
"content_authority": 0-100,
"answer_readiness": 0-100,
"direct_answer_quality": 0-100,
"findings": ["finding1", "finding2"],
"improvements": ["improvement1", "improvement2"]
}}

Only return the JSON, no other text."""

    try:
        print("[AEO] Calling Bedrock for visibility analysis...")
        
        # Format the request for Claude on Bedrock using the Messages API format
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,  # Reduced from 1500
            "temperature": 0.3,
            "messages": [
                {
                    "role": "user",
                    "content": visibility_prompt
                }
            ]
        }

        # Try primary model first with retry logic
        try:
            response = call_bedrock_with_retry(bedrock_runtime, PRIMARY_MODEL, request_body, max_retries=3)
        except Exception as e:
            print(f"[AEO] Primary model failed: {e}, trying fallback model...")
            # Try fallback model
            response = call_bedrock_with_retry(bedrock_runtime, FALLBACK_MODEL, request_body, max_retries=2)

        response_body = json.loads(response['body'].read().decode('utf-8'))
        print(f"[AEO] Bedrock response received")

        # Extract the text response for Claude format
        response_text = ""
        if 'content' in response_body:
            # Claude returns content as a list of content blocks
            if isinstance(response_body['content'], list) and len(response_body['content']) > 0:
                for content_block in response_body['content']:
                    if content_block.get('type') == 'text':
                        response_text = content_block.get('text', '')
                        break
        else:
            response_text = json.dumps(response_body)

        data = _parse_json(response_text)

        factors["ai_visibility"] = {
            "score": data.get("citation_likelihood", 50),
            "findings": data.get("findings", []),
            "label": "AI Citation Likelihood"
        }

        factors["content_authority"] = {
            "score": data.get("content_authority", 50),
            "findings": [f"Content authority score {data.get('content_authority',50)}/100"],
            "label": "Content Authority"
        }

        factors["answer_readiness"] = {
            "score": data.get("answer_readiness", 50),
            "findings": [f"Answer readiness {data.get('answer_readiness',50)}/100"],
            "label": "Answer Readiness"
        }

        factors["direct_answer"] = {
            "score": data.get("direct_answer_quality", 50),
            "findings": [f"Direct answer quality {data.get('direct_answer_quality',50)}/100"],
            "label": "Direct Answer Quality"
        }

        for imp in data.get("improvements", []):
            recommendations.append({
                "priority": "high",
                "category": "AEO",
                "action": imp
            })

    except Exception as e:
        print(f"[AEO] AI visibility check failed: {type(e).__name__}: {e}")
        print(traceback.format_exc())

        factors["ai_visibility"] = {
            "score": 50,
            "findings": [f"AI analysis partially completed: {str(e)[:100]}"],
            "label": "AI Citation Likelihood"
        }

        factors["content_authority"] = {
            "score": 50,
            "findings": ["Unable to fully assess"],
            "label": "Content Authority"
        }

        factors["answer_readiness"] = {
            "score": 50,
            "findings": ["Unable to fully assess"],
            "label": "Answer Readiness"
        }

        factors["direct_answer"] = {
            "score": 50,
            "findings": ["Unable to fully assess"],
            "label": "Direct Answer Quality"
        }

    # ---------- 2 FAQ Coverage ----------
    faq_prompt = f"""Analyze FAQ coverage.

Keywords: {', '.join(keywords)}
Industry: {industry}

Content:
{content_summary[:1500]}  # Reduced from 2000

Return your analysis in this exact JSON format:
{{
"faq_coverage_score": 0-100,
"missing_questions": ["question1", "question2"],
"faq_findings": ["finding1", "finding2"]
}}

Only return the JSON, no other text."""

    try:
        print("[AEO] Calling Bedrock for FAQ analysis...")
        
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 800,  # Reduced from 1000
            "temperature": 0.3,
            "messages": [
                {
                    "role": "user",
                    "content": faq_prompt
                }
            ]
        }

        # Try primary model first with retry logic
        try:
            response = call_bedrock_with_retry(bedrock_runtime, PRIMARY_MODEL, request_body, max_retries=3)
        except Exception as e:
            print(f"[AEO] Primary model failed for FAQ: {e}, trying fallback model...")
            response = call_bedrock_with_retry(bedrock_runtime, FALLBACK_MODEL, request_body, max_retries=2)

        response_body = json.loads(response['body'].read().decode('utf-8'))

        # Extract the text response for Claude format
        response_text = ""
        if 'content' in response_body:
            if isinstance(response_body['content'], list) and len(response_body['content']) > 0:
                for content_block in response_body['content']:
                    if content_block.get('type') == 'text':
                        response_text = content_block.get('text', '')
                        break
        else:
            response_text = json.dumps(response_body)

        faq_data = _parse_json(response_text)

        factors["faq_coverage"] = {
            "score": faq_data.get("faq_coverage_score", 40),
            "findings": faq_data.get("faq_findings", []),
            "label": "FAQ Coverage"
        }

        for q in faq_data.get("missing_questions", [])[:3]:
            recommendations.append({
                "priority": "medium",
                "category": "AEO FAQ",
                "action": f"Add content answering: '{q}'"
            })

    except Exception as e:
        print(f"[AEO] FAQ check failed: {type(e).__name__}: {e}")
        print(traceback.format_exc())

        factors["faq_coverage"] = {
            "score": 40,
            "findings": [f"FAQ analysis partially completed: {str(e)[:100]}"],
            "label": "FAQ Coverage"
        }

    # ---------- 3 Structured Answers ----------
    h_all = []
    for level in ["h1", "h2", "h3"]:
        h_all.extend(headings.get(level, []))

    question_words = [
        "what", "how", "why", "when", "where",
        "who", "which", "can", "does", "is"
    ]

    question_headings = [
        h for h in h_all
        if any(h.lower().startswith(w) for w in question_words)
    ]

    if len(question_headings) >= 3:
        struct_score = 90
    elif len(question_headings) >= 1:
        struct_score = 60
    else:
        struct_score = 30

    struct_findings = [
        f"{len(question_headings)} question headings found"
    ]

    structured = crawl_data.get("structured_data", [])

    faq_schema = any(s.get("type") == "FAQPage" for s in structured)

    if faq_schema:
        struct_score = min(100, struct_score + 20)
        struct_findings.append("FAQ schema detected")

    factors["structured_answers"] = {
        "score": struct_score,
        "findings": struct_findings,
        "label": "Structured Answers"
    }

    # ---------- Overall Score ----------
    weights = {
        "ai_visibility": 0.25,
        "content_authority": 0.20,
        "answer_readiness": 0.15,
        "direct_answer": 0.15,
        "faq_coverage": 0.15,
        "structured_answers": 0.10
    }

    overall_score = sum(
        factors[k]["score"] * weights.get(k, 0.1)
        for k in factors
    )

    if overall_score >= 75:
        summary = f"{brand_name} has strong AEO readiness ({overall_score:.0f}/100)"
    elif overall_score >= 50:
        summary = f"{brand_name} has moderate AEO readiness ({overall_score:.0f}/100)"
    else:
        summary = f"{brand_name} has low AEO readiness ({overall_score:.0f}/100)"

    return {
        "overall_score": round(overall_score, 1),
        "factors": factors,
        "recommendations": recommendations,
        "summary": summary
    }


def _get_fallback_result(brand_name, crawl_data):
    """Return fallback result when AI is unavailable"""
    headings = crawl_data.get("headings", {})
    
    # Calculate structured answers score (doesn't need AI)
    h_all = []
    for level in ["h1", "h2", "h3"]:
        h_all.extend(headings.get(level, []))
    
    question_words = ["what", "how", "why", "when", "where", "who", "which", "can", "does", "is"]
    question_headings = [h for h in h_all if any(h.lower().startswith(w) for w in question_words)]
    
    if len(question_headings) >= 3:
        struct_score = 90
    elif len(question_headings) >= 1:
        struct_score = 60
    else:
        struct_score = 30
    
    return {
        "overall_score": 40.0,
        "factors": {
            "ai_visibility": {"score": 50, "findings": ["AI service unavailable"], "label": "AI Citation Likelihood"},
            "content_authority": {"score": 50, "findings": ["AI service unavailable"], "label": "Content Authority"},
            "answer_readiness": {"score": 50, "findings": ["AI service unavailable"], "label": "Answer Readiness"},
            "direct_answer": {"score": 50, "findings": ["AI service unavailable"], "label": "Direct Answer Quality"},
            "faq_coverage": {"score": 40, "findings": ["AI service unavailable"], "label": "FAQ Coverage"},
            "structured_answers": {"score": struct_score, "findings": [f"{len(question_headings)} question headings found"], "label": "Structured Answers"}
        },
        "recommendations": [],
        "summary": f"{brand_name} AEO analysis incomplete - AI service unavailable"
    }


def _parse_json(text):
    """Parse JSON from LLM response, handling markdown code blocks"""
    text = text.strip()

    # Remove markdown code blocks
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        # Try to extract JSON object using regex
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        return {}