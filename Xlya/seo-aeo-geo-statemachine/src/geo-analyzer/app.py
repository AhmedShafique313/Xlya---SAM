"""
XYLA INSIGHTS — GEO Analyzer Lambda
Step Functions: GEO (Generative Engine Optimization) analysis.
Uses Amazon Bedrock Claude 3 Haiku + DuckDuckGo for AI presence scoring.
Runs inside the Parallel state. Sends WebSocket progress updates.
"""
import os
import json
import re
import boto3

from ws_helper import send_progress_update, update_analysis_status

# Initialize Bedrock client
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"


def lambda_handler(event, context):
    # Extract task_id from the input - it's at the root level
    task_id = event["task_id"]
    
    # Extract crawl_data and metadata from the nested structure
    crawl_data = event.get("crawl_data", {})
    
    # Get brand info from crawl_data or from metadata
    brand_name = crawl_data.get("brand_name", event.get("metadata", {}).get("brand_name", ""))
    keywords_str = crawl_data.get("keywords", event.get("metadata", {}).get("keywords", ""))
    keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]
    industry = crawl_data.get("industry", event.get("metadata", {}).get("industry", ""))
    
    # For backward compatibility, also check root level
    if not brand_name:
        brand_name = event.get("brand_name", "")
    if not keywords_str:
        keywords_str = event.get("keywords", "")
        keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]
    if not industry:
        industry = event.get("industry", "")

    print(f"[GEO] Starting GEO analysis for task {task_id}")

    send_progress_update(task_id, "analyzing_geo", 70, "Running GEO analysis & AI presence scan...")
    update_analysis_status(task_id, "analyzing_geo", 70)

    geo_result = _analyze_geo(crawl_data, brand_name, keywords, industry)

    score = geo_result["overall_score"]
    send_progress_update(task_id, "analyzing_geo", 80, f"GEO analysis complete — Score: {score}/100 ✓")
    update_analysis_status(task_id, "analyzing_geo", 80)

    return {
        "message": "Running GEO analysis & AI presence scan...",
        "geo_data": geo_result,
    }


def _analyze_geo(crawl_data, brand_name, keywords, industry):
    from duckduckgo_search import DDGS

    factors = {}
    recommendations = []

    # --- 1. AI Brand Mention Simulation using Bedrock Claude 3 Haiku ---
    brand_prompt = f"""You are simulating multiple AI search engines. A user asks about "{keywords[0] if keywords else ''}" in the "{industry}" industry.

Generate a realistic AI-generated answer to the query: "What are the best {keywords[0] if keywords else ''} solutions in {industry}?"

Then evaluate whether the brand "{brand_name}" ({crawl_data.get('url', '')}) would likely be mentioned.

Content excerpt: {crawl_data.get('markdown', '')[:2500]}

Respond in JSON format only, with no additional text before or after:
{{
  "brand_mentioned": true/false,
  "mention_likelihood_score": <0-100>,
  "brand_authority_signals": ["signal1", "signal2"],
  "content_uniqueness": <0-100>,
  "topical_authority": <0-100>,
  "ai_friendliness": <0-100>,
  "sample_ai_response_snippet": "...",
  "findings": ["finding1", "finding2"],
  "improvements": ["improvement1", "improvement2"]
}}"""

    try:
        # Call Bedrock Claude 3 Haiku
        response = bedrock_runtime.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1500,
                "temperature": 0.3,
                "messages": [
                    {
                        "role": "user",
                        "content": brand_prompt
                    }
                ]
            })
        )
        
        response_body = json.loads(response['body'].read())
        data = _parse_json(response_body['content'][0]['text'])

        factors["ai_brand_presence"] = {"score": data.get("mention_likelihood_score", 40), "findings": data.get("findings", ["AI brand presence analyzed"]), "label": "AI Brand Presence", "sample_response": data.get("sample_ai_response_snippet", "")}
        factors["content_uniqueness"] = {"score": data.get("content_uniqueness", 50), "findings": [f"Content uniqueness: {data.get('content_uniqueness', 50)}/100"], "label": "Content Uniqueness"}
        factors["topical_authority"] = {"score": data.get("topical_authority", 50), "findings": [f"Topical authority: {data.get('topical_authority', 50)}/100"] + data.get("brand_authority_signals", []), "label": "Topical Authority"}
        factors["ai_friendliness"] = {"score": data.get("ai_friendliness", 50), "findings": [f"AI-friendliness: {data.get('ai_friendliness', 50)}/100"], "label": "AI Content Friendliness"}

        for imp in data.get("improvements", []):
            recommendations.append({"priority": "high", "category": "GEO", "action": imp})
    except Exception as e:
        print(f"[GEO] Brand simulation error: {e}")
        factors["ai_brand_presence"] = {"score": 40, "findings": ["Partial analysis"], "label": "AI Brand Presence"}
        factors["content_uniqueness"] = {"score": 50, "findings": ["Unable to fully assess"], "label": "Content Uniqueness"}
        factors["topical_authority"] = {"score": 50, "findings": ["Unable to fully assess"], "label": "Topical Authority"}
        factors["ai_friendliness"] = {"score": 50, "findings": ["Unable to fully assess"], "label": "AI Content Friendliness"}

    # --- 2. Web Presence via DuckDuckGo ---
    web_presence_score = 0
    web_findings = []
    citation_count = 0
    try:
        ddgs = DDGS()
        search_query = f'"{brand_name}" {keywords[0] if keywords else ""}'
        brand_results = list(ddgs.text(search_query, max_results=10))
        citation_count = len(brand_results)

        if citation_count >= 8:
            web_presence_score = 95
            web_findings.append(f"Strong web presence: {citation_count} mentions found")
        elif citation_count >= 5:
            web_presence_score = 75
            web_findings.append(f"Good web presence: {citation_count} mentions found")
        elif citation_count >= 2:
            web_presence_score = 50
            web_findings.append(f"Moderate web presence: {citation_count} mentions found")
            recommendations.append({"priority": "high", "category": "GEO - Web Presence", "action": "Increase online presence through content marketing, PR, and guest posts"})
        else:
            web_presence_score = 20
            web_findings.append(f"Low web presence: only {citation_count} mentions found")
            recommendations.append({"priority": "critical", "category": "GEO - Web Presence", "action": "Urgently build web presence through content distribution and PR"})
    except Exception as e:
        print(f"[GEO] DuckDuckGo error: {e}")
        web_presence_score = 40
        web_findings.append("Web presence check partially completed")

    factors["web_presence"] = {"score": web_presence_score, "findings": web_findings, "label": "Web Presence & Citations"}

    # --- 3. AI Presence Index using Bedrock Claude 3 Haiku ---
    ai_presence_prompt = f"""Calculate an AI Presence Index for "{brand_name}" in "{industry}" for keywords: {', '.join(keywords) if keywords else ''}.

Consider:
- Web presence: {citation_count} search results found
- Content: {crawl_data.get('word_count', 0)} words, {len(crawl_data.get('structured_data', []))} schemas
- Content sample: {crawl_data.get('markdown', '')[:1500]}

Respond in JSON format only, with no additional text before or after:
{{
  "ai_presence_index": <0-100>,
  "chatgpt_likelihood": <0-100>,
  "google_ai_likelihood": <0-100>,
  "perplexity_likelihood": <0-100>,
  "explanation": "...",
  "top_recommendations": ["rec1", "rec2", "rec3"]
}}"""

    ai_presence_index = 40
    try:
        ai_response = bedrock_runtime.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 800,
                "temperature": 0.3,
                "messages": [
                    {
                        "role": "user",
                        "content": ai_presence_prompt
                    }
                ]
            })
        )
        
        ai_response_body = json.loads(ai_response['body'].read())
        ai_data = _parse_json(ai_response_body['content'][0]['text'])
        ai_presence_index = ai_data.get("ai_presence_index", 40)

        factors["ai_presence_index"] = {
            "score": ai_presence_index,
            "findings": [
                ai_data.get("explanation", "AI presence analyzed"),
                f"ChatGPT: {ai_data.get('chatgpt_likelihood', 'N/A')}/100",
                f"Google AI: {ai_data.get('google_ai_likelihood', 'N/A')}/100",
                f"Perplexity: {ai_data.get('perplexity_likelihood', 'N/A')}/100",
            ],
            "label": "AI Presence Index",
            "platform_scores": {
                "chatgpt": ai_data.get("chatgpt_likelihood", 40),
                "google_ai": ai_data.get("google_ai_likelihood", 40),
                "perplexity": ai_data.get("perplexity_likelihood", 40),
            }
        }
        for rec in ai_data.get("top_recommendations", []):
            recommendations.append({"priority": "high", "category": "GEO - AI Presence", "action": rec})
    except Exception as e:
        print(f"[GEO] AI Presence error: {e}")
        factors["ai_presence_index"] = {"score": 40, "findings": ["AI presence index could not be fully calculated"], "label": "AI Presence Index", "platform_scores": {"chatgpt": 40, "google_ai": 40, "perplexity": 40}}

    # Overall GEO Score
    weights = {"ai_brand_presence": 0.25, "content_uniqueness": 0.15, "topical_authority": 0.20, "ai_friendliness": 0.10, "web_presence": 0.15, "ai_presence_index": 0.15}
    overall_score = sum(factors[k]["score"] * weights.get(k, 0.1) for k in factors if k in weights)

    if overall_score >= 75:
        summary = f"{brand_name} has excellent GEO ({overall_score:.0f}/100). AI Presence Index: {ai_presence_index:.0f}/100."
    elif overall_score >= 50:
        summary = f"{brand_name} has moderate GEO readiness ({overall_score:.0f}/100). AI Presence Index: {ai_presence_index:.0f}/100."
    else:
        summary = f"{brand_name} has low GEO readiness ({overall_score:.0f}/100). AI Presence Index: {ai_presence_index:.0f}/100."

    return {
        "overall_score": round(overall_score, 1),
        "ai_presence_index": ai_presence_index,
        "factors": factors,
        "recommendations": recommendations,
        "summary": summary,
    }


def _parse_json(text):
    text = text.strip()
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except Exception:
                pass
        return {}