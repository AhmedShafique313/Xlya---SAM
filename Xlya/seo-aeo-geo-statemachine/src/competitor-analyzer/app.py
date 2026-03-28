"""
XYLA INSIGHTS — Competitor Analyzer Lambda
Step Functions: Competitor discovery and comparison using DuckDuckGo + Claude 3 Haiku (Amazon Bedrock).
Runs inside the Parallel state. Returns running status message.
"""
import os
import json
import re
import boto3
from urllib.parse import urlparse
from duckduckgo_search import DDGS

# Initialize Bedrock client
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-east-1')
MODEL_ID = "openai.gpt-oss-120b-1:0"


def lambda_handler(event, context):
    task_id = event["task_id"]
    
    # Extract data from the input event
    crawl_data = event.get("crawl_data", {})
    metadata = event.get("metadata", {})
    
    brand_name = metadata.get("brand_name", "")
    keywords_str = metadata.get("keywords", "")
    keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]
    industry = metadata.get("industry", "")

    print(f"[Competitor] Starting competitor analysis for task {task_id}")

    # Return running status message as per requirement
    return {
        "status": "running",
        "message": "Running GEO analysis & AI presence scan...",
        "task_id": task_id,
        "competitor_data": None  # Will be populated in subsequent execution
    }


def _analyze_competitors(crawl_data, brand_name, keywords, industry):
    competitors = []

    # --- Find competitors via DuckDuckGo ---
    try:
        ddgs = DDGS()
        search_query = f"best {keywords[0]} {industry}" if keywords else f"best {industry}"
        results = list(ddgs.text(search_query, max_results=10))

        brand_domain = urlparse(crawl_data.get("url", "")).netloc.lower()
        seen = set()

        skip_domains = [
            "wikipedia.org", "youtube.com", "reddit.com", "quora.com",
            "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
            "amazon.com", brand_domain
        ]

        for r in results:
            href = r.get("href", "")
            domain = urlparse(href).netloc.lower()
            if domain and domain not in seen and not any(s in domain for s in skip_domains):
                seen.add(domain)
                competitors.append({
                    "domain": domain,
                    "url": href,
                    "title": r.get("title", ""),
                    "description": r.get("body", "")[:200],
                })
            if len(competitors) >= 5:
                break
    except Exception as e:
        print(f"[Competitor] DuckDuckGo error: {e}")

    if not competitors:
        return {
            "competitors_found": 0,
            "competitors": [],
            "comparison": {},
            "summary": f"No direct competitors found for {brand_name}.",
            "recommendations": [{"priority": "low", "category": "Competitor Analysis", "action": "Unable to identify competitors — refine keywords"}],
        }

    # --- AI Competitive Analysis using Claude 3 Haiku ---
    comp_list = "\n".join([f"- {c['domain']}: {c['title']}" for c in competitors[:5]])

    prompt = f"""You are a competitive analysis expert. Compare "{brand_name}" ({crawl_data.get('url', '')}) against these competitors for "{keywords[0] if keywords else ''}" in "{industry}":

Competitors:
{comp_list}

Brand info:
Title: {crawl_data.get('title', '')}
Description: {crawl_data.get('description', '')}
Content length: {crawl_data.get('word_count', 0)} words
Structured data: {len(crawl_data.get('structured_data', []))} schemas

Provide a competitive analysis in the following JSON format. Return ONLY valid JSON, no other text:
{{
  "competitors": [
    {{
      "name": "domain",
      "estimated_seo_score": <0-100>,
      "estimated_aeo_score": <0-100>,
      "estimated_geo_score": <0-100>,
      "strengths": ["s1", "s2"],
      "weaknesses": ["w1"]
    }}
  ],
  "brand_position": "top/middle/bottom",
  "brand_advantages": ["a1", "a2"],
  "brand_disadvantages": ["d1"],
  "key_insights": ["i1", "i2", "i3"],
  "strategic_recommendations": ["r1", "r2"]
}}"""

    recommendations = []
    comparison = {}

    try:
        # Invoke Claude 3 Haiku via Bedrock
        response = bedrock_runtime.invoke_model(
            modelId=MODEL_ID,
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2000,
                "temperature": 0.3,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            })
        )
        
        response_body = json.loads(response['body'].read())
        ai_response = response_body['content'][0]['text']
        
        data = _parse_json(ai_response)

        ai_comps = data.get("competitors", [])
        for i, comp in enumerate(competitors[:5]):
            if i < len(ai_comps):
                comp.update(ai_comps[i])
            else:
                comp.update({"estimated_seo_score": 50, "estimated_aeo_score": 50, "estimated_geo_score": 50, "strengths": [], "weaknesses": []})

        comparison = {
            "brand_position": data.get("brand_position", "middle"),
            "brand_advantages": data.get("brand_advantages", []),
            "brand_disadvantages": data.get("brand_disadvantages", []),
            "key_insights": data.get("key_insights", []),
        }

        for rec in data.get("strategic_recommendations", []):
            recommendations.append({"priority": "medium", "category": "Competitor Strategy", "action": rec})
            
    except Exception as e:
        print(f"[Competitor] AI analysis error: {e}")
        for comp in competitors:
            comp.update({"estimated_seo_score": 50, "estimated_aeo_score": 50, "estimated_geo_score": 50, "strengths": [], "weaknesses": []})
        comparison = {"brand_position": "unknown", "brand_advantages": [], "brand_disadvantages": [], "key_insights": ["Competitive analysis was partially completed"]}

    return {
        "competitors_found": len(competitors),
        "competitors": competitors[:5],
        "comparison": comparison,
        "recommendations": recommendations,
        "summary": f"Found {len(competitors)} competitors for {brand_name}. Position: {comparison.get('brand_position', 'N/A')}.",
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