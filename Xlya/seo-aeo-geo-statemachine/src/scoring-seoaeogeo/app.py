"""
XYLA INSIGHTS — SEO Analyzer Lambda
Step Functions Step 2: Performs technical SEO audit on crawled website data.
"""

def lambda_handler(event, context):
    """
    Expected Input from Step Functions:

    {
      "task_id": "...",
      "crawl_data": {...},
      "metadata": {
          "brand_name": "...",
          "keywords": "kw1, kw2",
          "industry": "..."
      }
    }
    """

    task_id = event.get("task_id")

    metadata = event.get("metadata", {})
    keywords_str = metadata.get("keywords", "")
    keywords = [kw.strip() for kw in keywords_str.split(",") if kw.strip()]

    crawl_data = event.get("crawl_data", {})

    print(f"[SEO] Starting SEO analysis for task {task_id}")
    
    # Print status message
    status_message = "Running SEO audit..."
    print(status_message)

    seo_result = _analyze_seo(crawl_data, keywords)

    return {
        "task_id": task_id,
        "seo_data": seo_result,
        "message": status_message  # Add the message to the output
    }


def _analyze_seo(crawl_data: dict, keywords: list) -> dict:
    # ... rest of your existing code remains exactly the same ...
    factors = {}
    recommendations = []

    # 1. Title Tag
    title = crawl_data.get("title", "")
    title_score = 0
    title_findings = []

    if title:
        title_len = len(title)

        if 30 <= title_len <= 60:
            title_score = 100
            title_findings.append("Title length is optimal (30-60 chars)")
        elif 20 <= title_len < 30 or 60 < title_len <= 70:
            title_score = 70
            title_findings.append(f"Title length ({title_len} chars) is slightly outside optimal range")
        else:
            title_score = 40
            title_findings.append(f"Title length ({title_len} chars) needs improvement")

            recommendations.append({
                "priority": "high",
                "category": "Title Tag",
                "action": f"Optimize title tag length to 30-60 characters (currently {title_len})"
            })

        kw_in_title = any(kw.lower() in title.lower() for kw in keywords)

        if kw_in_title:
            title_score = min(100, title_score + 10)
            title_findings.append("Target keyword found in title ✓")
        else:
            title_score = max(0, title_score - 15)
            title_findings.append("Target keyword NOT found in title")

            recommendations.append({
                "priority": "high",
                "category": "Title Tag",
                "action": "Include primary keyword in title tag"
            })

    else:
        title_findings.append("No title tag found!")

        recommendations.append({
            "priority": "critical",
            "category": "Title Tag",
            "action": "Add a title tag to the page"
        })

    factors["title_tag"] = {
        "score": title_score,
        "findings": title_findings,
        "label": "Title Tag"
    }

    # 2. Meta Description
    description = crawl_data.get("description") or ""
    desc_score = 0
    desc_findings = []

    if description:
        desc_len = len(description)

        if 120 <= desc_len <= 160:
            desc_score = 100
            desc_findings.append("Meta description length is optimal")
        elif 80 <= desc_len < 120 or 160 < desc_len <= 200:
            desc_score = 70
            desc_findings.append(f"Meta description ({desc_len} chars) could be optimized")
        else:
            desc_score = 40
            desc_findings.append(f"Meta description ({desc_len} chars) needs improvement")

            recommendations.append({
                "priority": "medium",
                "category": "Meta Description",
                "action": f"Optimize meta description to 120-160 characters (currently {desc_len})"
            })

        kw_in_desc = any(kw.lower() in description.lower() for kw in keywords)

        if kw_in_desc:
            desc_score = min(100, desc_score + 10)
            desc_findings.append("Target keyword found in meta description ✓")
        else:
            desc_findings.append("Consider adding target keyword to meta description")

            recommendations.append({
                "priority": "medium",
                "category": "Meta Description",
                "action": "Include target keywords in meta description"
            })

    else:
        desc_findings.append("No meta description found!")

        recommendations.append({
            "priority": "critical",
            "category": "Meta Description",
            "action": "Add a meta description tag"
        })

    factors["meta_description"] = {
        "score": desc_score,
        "findings": desc_findings,
        "label": "Meta Description"
    }

    # 3. Heading Structure
    headings = crawl_data.get("headings", {})
    heading_score = 0
    heading_findings = []

    h1_list = headings.get("h1", [])
    h2_list = headings.get("h2", [])

    if len(h1_list) == 1:
        heading_score += 40
        heading_findings.append("Single H1 tag found ✓")

    elif len(h1_list) == 0:
        heading_findings.append("No H1 tag found!")

        recommendations.append({
            "priority": "critical",
            "category": "Headings",
            "action": "Add a single H1 heading to the page"
        })

    else:
        heading_score += 20
        heading_findings.append(f"Multiple H1 tags found ({len(h1_list)}) — ideally use only one")

        recommendations.append({
            "priority": "high",
            "category": "Headings",
            "action": "Use only one H1 tag per page"
        })

    if len(h2_list) >= 2:
        heading_score += 30
        heading_findings.append(f"{len(h2_list)} H2 subheadings found ✓")

    elif len(h2_list) == 1:
        heading_score += 20
        heading_findings.append("Only 1 H2 found — consider adding more")

    else:
        heading_findings.append("No H2 subheadings found")

        recommendations.append({
            "priority": "medium",
            "category": "Headings",
            "action": "Add H2 subheadings to structure content"
        })

    if h1_list and any(kw.lower() in h1_list[0].lower() for kw in keywords):
        heading_score += 30
        heading_findings.append("Target keyword found in H1 ✓")

    elif h1_list:
        heading_findings.append("Consider including target keyword in H1")

        recommendations.append({
            "priority": "medium",
            "category": "Headings",
            "action": "Include primary keyword in H1 heading"
        })

    factors["heading_structure"] = {
        "score": min(100, heading_score),
        "findings": heading_findings,
        "label": "Heading Structure"
    }

    # Content Quality
    word_count = crawl_data.get("word_count", 0)

    content_score = 100 if word_count >= 1500 else \
                    80 if word_count >= 800 else \
                    50 if word_count >= 300 else 20

    factors["content_quality"] = {
        "score": content_score,
        "findings": [f"Content length: {word_count} words"],
        "label": "Content Quality"
    }

    # Images
    images = crawl_data.get("images", {})
    total = images.get("total", 0)
    with_alt = images.get("with_alt", 0)

    img_score = 100 if total and (with_alt / total) > 0.8 else 50

    factors["image_optimization"] = {
        "score": img_score,
        "findings": [f"{with_alt}/{total} images have alt text"],
        "label": "Image Optimization"
    }

    # Links
    links = crawl_data.get("links", {})
    internal = links.get("internal", 0)

    link_score = 100 if internal >= 5 else 60 if internal >= 1 else 20

    factors["link_analysis"] = {
        "score": link_score,
        "findings": [f"{internal} internal links found"],
        "label": "Link Analysis"
    }

    # Technical SEO
    tech_score = 0

    if crawl_data.get("has_canonical"):
        tech_score += 40

    if crawl_data.get("has_robots_meta"):
        tech_score += 30

    if crawl_data.get("structured_data"):
        tech_score += 30

    factors["technical_seo"] = {
        "score": tech_score,
        "findings": [],
        "label": "Technical SEO"
    }

    weights = {
        "title_tag": 0.15,
        "meta_description": 0.10,
        "heading_structure": 0.15,
        "content_quality": 0.25,
        "image_optimization": 0.10,
        "link_analysis": 0.10,
        "technical_seo": 0.15
    }

    overall_score = sum(factors[k]["score"] * weights[k] for k in weights)

    return {
        "overall_score": round(overall_score, 1),
        "factors": factors,
        "recommendations": recommendations
    }