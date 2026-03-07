"""
XYLA INSIGHTS — Crawler Lambda
Triggered by S3 upload of a TXT initialization file.
Reads the TXT file, extracts task data, then crawls the website using Firecrawl API.
After successful crawl, triggers the seo-aeo-geo-SM Step Functions state machine.
"""

import os
import json
import time
import boto3
from botocore.exceptions import ClientError

FIRECRAWL_API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")  # Add this env variable
S3_PREFIX = "seo-aeo-geo-analyzer"

s3_client = boto3.client("s3")
stepfunctions_client = boto3.client("stepfunctions")  # Initialize Step Functions client


def lambda_handler(event, context):

    # ===============================
    # 1️⃣ Get S3 Event Info
    # ===============================

    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    print(f"[Crawler] Triggered by S3 object: {object_key}")

    # ===============================
    # 2️⃣ Read TXT file from S3
    # ===============================

    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )

        file_content = response["Body"].read().decode("utf-8")

    except Exception as e:
        print(f"[Crawler] Failed to read file from S3: {e}")
        raise e

    print("[Crawler] File content loaded")

    # ===============================
    # 3️⃣ Parse TXT content
    # ===============================

    parsed_data = {}

    for line in file_content.split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            parsed_data[key.strip().lower().replace(" ", "_")] = value.strip()

    task_id = parsed_data.get("task_id")
    url = parsed_data.get("url")
    brand_name = parsed_data.get("brand_name")
    keywords = parsed_data.get("keywords")
    industry = parsed_data.get("industry")

    print(f"[Crawler] Task ID: {task_id}")
    print(f"[Crawler] URL: {url}")

    # ===============================
    # 4️⃣ Start Crawling
    # ===============================

    crawl_data = {}
    crawl_success = False
    s3_key = None

    try:
        from firecrawl import FirecrawlApp
        from bs4 import BeautifulSoup

        app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)

        scrape_result = app.scrape_url(
            url,
            params={
                "formats": ["markdown", "html"],
                "onlyMainContent": False,
            },
        )

        metadata = scrape_result.get("metadata", {})
        html_content = scrape_result.get("html", "")
        markdown_content = scrape_result.get("markdown", "")

        soup = BeautifulSoup(html_content, "html.parser") if html_content else None

        crawl_data = {
            "task_id": task_id,
            "url": url,
            "brand_name": brand_name,
            "keywords": keywords,
            "industry": industry,
            "title": metadata.get("title", ""),
            "description": metadata.get("description", ""),
            "ogTitle": metadata.get("og:title", ""),
            "ogDescription": metadata.get("og:description", ""),
            "ogImage": metadata.get("og:image", ""),
            "language": metadata.get("language", ""),
            "statusCode": metadata.get("statusCode", 200),
            "markdown": markdown_content[:15000] if markdown_content else "",
            "html_snippet": html_content[:5000] if html_content else "",
            "headings": _extract_headings(soup),
            "links": _extract_links(soup, url),
            "images": _extract_images(soup),
            "meta_tags": _extract_meta_tags(soup),
            "structured_data": _extract_structured_data(soup),
            "word_count": len(markdown_content.split()) if markdown_content else 0,
            "has_robots_meta": _check_robots_meta(soup),
            "has_canonical": _check_canonical(soup),
            "has_sitemap_link": _check_sitemap(soup),
        }

        crawl_success = True

        # ===============================
        # 5️⃣ Store Crawl Result to S3
        # ===============================

        if S3_BUCKET:
            try:
                s3_key = f"crawls/{task_id}/raw_data.json"
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=json.dumps(crawl_data, default=str),
                    ContentType="application/json",
                )
                print(f"[Crawler] Successfully stored crawl data to s3://{S3_BUCKET}/{s3_key}")
            except ClientError as e:
                print(f"[Crawler] S3 storage warning: {e}")

    except Exception as e:
        print(f"[Crawler] Error during crawl: {e}")
        crawl_data = _get_fallback_data(url)

    # ===============================
    # 6️⃣ Trigger Step Functions State Machine
    # ===============================
    
    if crawl_success and STATE_MACHINE_ARN:
        try:
            # Create a unique execution name using task_id and timestamp
            execution_name = f"{task_id}-{int(time.time())}"
            
            # Prepare input for the state machine
            state_machine_input = {
                "task_id": task_id,
                "crawl_data": crawl_data,
                "s3_location": {
                    "bucket": S3_BUCKET,
                    "key": s3_key,
                    "url": f"s3://{S3_BUCKET}/{s3_key}" if s3_key else None
                },
                "metadata": {
                    "brand_name": brand_name,
                    "keywords": keywords,
                    "industry": industry,
                    "crawl_timestamp": time.time()
                }
            }
            
            # Start the state machine execution
            response = stepfunctions_client.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name,
                input=json.dumps(state_machine_input)
            )
            
            print(f"[Crawler] Successfully started state machine execution: {response['executionArn']}")
            
        except Exception as e:
            print(f"[Crawler] Failed to start state machine: {e}")
            # Don't fail the Lambda if state machine trigger fails
            # But log it for monitoring
    elif not STATE_MACHINE_ARN:
        print("[Crawler] STATE_MACHINE_ARN not configured. Skipping state machine trigger.")
    else:
        print("[Crawler] Crawl was not successful. Skipping state machine trigger.")

    return {
        "success": crawl_success,
        "message": "Crawling website completed",
        "crawl_data": crawl_data,
        "state_machine_triggered": crawl_success and bool(STATE_MACHINE_ARN),
        "s3_location": f"s3://{S3_BUCKET}/{s3_key}" if s3_key else None
    }


# ============================================================
# HTML Parsing Helpers (Keep all your existing helper functions)
# ============================================================

def _extract_headings(soup):
    if not soup:
        return {"h1": [], "h2": [], "h3": [], "h4": [], "h5": [], "h6": []}

    headings = {}

    for level in range(1, 7):
        tag = f"h{level}"
        headings[tag] = [h.get_text(strip=True) for h in soup.find_all(tag)]

    return headings


def _extract_links(soup, base_url):

    if not soup:
        return {"internal": 0, "external": 0, "nofollow": 0, "total": 0}

    from urllib.parse import urlparse

    base_domain = urlparse(base_url).netloc
    links = soup.find_all("a", href=True)

    internal = external = nofollow = 0

    for link in links:
        href = link.get("href", "")
        rel = link.get("rel", [])

        if "nofollow" in rel:
            nofollow += 1

        parsed = urlparse(href)

        if parsed.netloc == "" or parsed.netloc == base_domain:
            internal += 1
        else:
            external += 1

    return {
        "internal": internal,
        "external": external,
        "nofollow": nofollow,
        "total": len(links),
    }


def _extract_images(soup):

    if not soup:
        return {"total": 0, "with_alt": 0, "without_alt": 0, "alt_texts": []}

    images = soup.find_all("img")

    with_alt = sum(1 for img in images if img.get("alt", "").strip())
    alt_texts = [img.get("alt", "") for img in images if img.get("alt", "").strip()]

    return {
        "total": len(images),
        "with_alt": with_alt,
        "without_alt": len(images) - with_alt,
        "alt_texts": alt_texts[:20],
    }


def _extract_meta_tags(soup):

    if not soup:
        return []

    metas = []

    for meta in soup.find_all("meta"):
        tag_data = {}

        for attr in ["name", "property", "content", "charset", "http-equiv"]:
            val = meta.get(attr)
            if val:
                tag_data[attr] = val

        if tag_data:
            metas.append(tag_data)

    return metas[:30]


def _extract_structured_data(soup):

    if not soup:
        return []

    schemas = []

    for script in soup.find_all("script", type="application/ld+json"):

        try:
            data = json.loads(script.string)

            if isinstance(data, dict):
                schemas.append({"type": data.get("@type", "Unknown"), "found": True})

            elif isinstance(data, list):

                for item in data:
                    if isinstance(item, dict):
                        schemas.append({"type": item.get("@type", "Unknown"), "found": True})

        except Exception:
            pass

    return schemas


def _check_robots_meta(soup):

    if not soup:
        return False

    return soup.find("meta", attrs={"name": "robots"}) is not None


def _check_canonical(soup):

    if not soup:
        return False

    return soup.find("link", attrs={"rel": "canonical"}) is not None


def _check_sitemap(soup):

    if not soup:
        return False

    return "sitemap" in str(soup).lower()


def _get_fallback_data(url):

    return {
        "url": url,
        "title": "",
        "description": "",
        "ogTitle": "",
        "ogDescription": "",
        "ogImage": "",
        "language": "",
        "statusCode": 0,
        "markdown": "",
        "html_snippet": "",
        "headings": {"h1": [], "h2": [], "h3": [], "h4": [], "h5": [], "h6": []},
        "links": {"internal": 0, "external": 0, "nofollow": 0, "total": 0},
        "images": {"total": 0, "with_alt": 0, "without_alt": 0, "alt_texts": []},
        "meta_tags": [],
        "structured_data": [],
        "word_count": 0,
        "has_robots_meta": False,
        "has_canonical": False,
        "has_sitemap_link": False,
    }