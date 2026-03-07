import os
import json
import re

DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")


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
        "aeo_data": aeo_result
    }


def _analyze_aeo(crawl_data, brand_name, keywords, industry):
    from openai import OpenAI

    client = OpenAI(
        api_key=DEEPSEEK_API_KEY,
        base_url="https://api.deepseek.com"
    )

    factors = {}
    recommendations = []

    content_summary = crawl_data.get("markdown", "")[:5000]
    title = crawl_data.get("title", "")
    description = crawl_data.get("description", "")
    headings = crawl_data.get("headings", {})

    # ---------- 1 AI Visibility ----------
    visibility_prompt = f"""
You are an AI search engine evaluator.

Analyze whether this website content would be cited by AI answer engines
(ChatGPT, Google AI Overview, Perplexity).

Keywords: {', '.join(keywords)}
Industry: {industry}

Brand: {brand_name}
Title: {title}
Description: {description}

Content:
{content_summary[:3000]}

Return JSON:

{{
"citation_likelihood": 0-100,
"content_authority": 0-100,
"answer_readiness": 0-100,
"direct_answer_quality": 0-100,
"findings": [],
"improvements": []
}}
"""

    try:

        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "Respond only with JSON."},
                {"role": "user", "content": visibility_prompt}
            ],
            temperature=0.3,
            max_tokens=1500
        )

        data = _parse_json(resp.choices[0].message.content)

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

        print("[AEO] AI visibility check failed:", e)

        factors["ai_visibility"] = {
            "score": 50,
            "findings": ["AI analysis partially completed"],
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

    faq_prompt = f"""
Analyze FAQ coverage.

Keywords: {', '.join(keywords)}
Industry: {industry}

Content:
{content_summary[:2000]}

Return JSON:

{{
"faq_coverage_score": 0-100,
"missing_questions": [],
"faq_findings": []
}}
"""

    try:

        faq_resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "Respond only with JSON."},
                {"role": "user", "content": faq_prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )

        faq_data = _parse_json(faq_resp.choices[0].message.content)

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

        print("[AEO] FAQ check failed:", e)

        factors["faq_coverage"] = {
            "score": 40,
            "findings": ["FAQ analysis partially completed"],
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