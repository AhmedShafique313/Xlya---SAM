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
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN", "")
S3_PREFIX = "seo-aeo-geo-analyzer"

s3_client = boto3.client("s3")
stepfunctions_client = boto3.client("stepfunctions")


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
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
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
    # 4️⃣ Start Crawling (FIXED)
    # ===============================
    crawl_data = {}
    crawl_success = False
    s3_key = None

    try:
        # ✅ CORRECT import based on docs
        from firecrawl import Firecrawl
        from bs4 import BeautifulSoup

        # Initialize the Firecrawl client
        firecrawl = Firecrawl(api_key=FIRECRAWL_API_KEY)

        print(f"[Crawler] Starting scrape for URL: {url}")

        # ✅ FIX: Use snake_case parameters and handle Document object response
        scrape_result = firecrawl.scrape(
            url,
            formats=["markdown", "html"],      # List of strings as per docs
            only_main_content=False,            # ✅ CORRECT: snake_case parameter
            timeout=120000                      # Timeout in milliseconds
        )

        # Debug: Log the type of result (helpful for troubleshooting)
        print(f"[Crawler] Type of scrape_result: {type(scrape_result)}")

        if not scrape_result:
            raise Exception("No data returned from Firecrawl")

        # ✅ FIX: Handle Document object correctly (access attributes directly)
        # The Firecrawl SDK returns a Document object with attributes, not a dictionary
        
        # Access metadata - could be dict or object
        if hasattr(scrape_result, 'metadata'):
            metadata_obj = scrape_result.metadata
            # Convert metadata to dictionary if it's an object
            if metadata_obj and not isinstance(metadata_obj, dict):
                metadata = {}
                # Common metadata attributes from Firecrawl docs
                for attr in ['title', 'description', 'ogTitle', 'ogDescription', 'ogImage', 
                           'language', 'sourceURL', 'statusCode', 'robots', 'keywords']:
                    if hasattr(metadata_obj, attr):
                        metadata[attr] = getattr(metadata_obj, attr)
            else:
                metadata = metadata_obj or {}
        else:
            metadata = {}

        # Access html content
        html_content = ""
        if hasattr(scrape_result, 'html'):
            html_content = scrape_result.html or ""
        elif hasattr(scrape_result, 'rawHtml'):
            html_content = scrape_result.rawHtml or ""

        # Access markdown content
        markdown_content = ""
        if hasattr(scrape_result, 'markdown'):
            markdown_content = scrape_result.markdown or ""

        print(f"[Crawler] Successfully scraped {url}")
        print(f"[Crawler] Content length - HTML: {len(html_content)}, Markdown: {len(markdown_content)}")
        print(f"[Crawler] Metadata keys: {list(metadata.keys()) if isinstance(metadata, dict) else 'Not a dict'}")

        # Parse HTML with BeautifulSoup
        soup = None
        if html_content:
            try:
                soup = BeautifulSoup(html_content, "html.parser")
            except Exception as e:
                print(f"[Crawler] Warning: Could not parse HTML: {e}")

        # Build comprehensive crawl data with safe dictionary access
        crawl_data = {
            "task_id": task_id,
            "url": url,
            "brand_name": brand_name,
            "keywords": keywords,
            "industry": industry,
            # Safely get metadata values
            "title": metadata.get('title', '') if isinstance(metadata, dict) else '',
            "description": metadata.get('description', '') if isinstance(metadata, dict) else '',
            "ogTitle": metadata.get('ogTitle', '') if isinstance(metadata, dict) else '',
            "ogDescription": metadata.get('ogDescription', '') if isinstance(metadata, dict) else '',
            "ogImage": metadata.get('ogImage', '') if isinstance(metadata, dict) else '',
            "language": metadata.get('language', '') if isinstance(metadata, dict) else '',
            "statusCode": metadata.get('statusCode', 200) if isinstance(metadata, dict) else 200,
            "sourceURL": metadata.get('sourceURL', url) if isinstance(metadata, dict) else url,
            # Content
            "markdown": markdown_content[:15000] if markdown_content else "",
            "html_snippet": html_content[:5000] if html_content else "",
            # Extracted data from BeautifulSoup
            "headings": _extract_headings(soup),
            "links": _extract_links(soup, url),
            "images": _extract_images(soup),
            "meta_tags": _extract_meta_tags(soup),
            "structured_data": _extract_structured_data(soup),
            "word_count": len(markdown_content.split()) if markdown_content else 0,
            "has_robots_meta": _check_robots_meta(soup),
            "has_canonical": _check_canonical(soup),
            "has_sitemap_link": _check_sitemap(soup),
            "crawl_timestamp": time.time(),
        }

        crawl_success = True
        print(f"[Crawler] Successfully processed {url}")

        # ===============================
        # 5️⃣ Store Crawl Result to S3
        # ===============================
        if S3_BUCKET and task_id:
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

    except ImportError as e:
        print(f"[Crawler] Critical: Firecrawl library not properly installed: {e}")
        crawl_data = _get_fallback_data(url)
        crawl_data["error"] = f"Library import error: {str(e)}"
    except Exception as e:
        print(f"[Crawler] Error during crawl: {e}")
        crawl_data = _get_fallback_data(url)
        crawl_data["error"] = str(e)

    # ===============================
    # 6️⃣ Trigger Step Functions State Machine
    # ===============================
    if crawl_success and STATE_MACHINE_ARN:
        try:
            # Create a unique execution name
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
    elif not STATE_MACHINE_ARN:
        print("[Crawler] STATE_MACHINE_ARN not configured. Skipping state machine trigger.")
    else:
        print("[Crawler] Crawl was not successful. Skipping state machine trigger.")

    return {
        "success": crawl_success,
        "message": "Crawling website completed" if crawl_success else "Crawl failed",
        "crawl_data": crawl_data,
        "state_machine_triggered": crawl_success and bool(STATE_MACHINE_ARN),
        "s3_location": f"s3://{S3_BUCKET}/{s3_key}" if s3_key else None
    }


# ============================================================
# HTML Parsing Helpers (These remain the same - they work correctly)
# ============================================================

def _extract_headings(soup):
    """Extract all headings (h1-h6) from the page"""
    if not soup:
        return {"h1": [], "h2": [], "h3": [], "h4": [], "h5": [], "h6": []}

    headings = {}
    for level in range(1, 7):
        tag = f"h{level}"
        headings[tag] = [h.get_text(strip=True) for h in soup.find_all(tag)][:20]

    return headings


def _extract_links(soup, base_url):
    """Extract and categorize links from the page"""
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
    """Extract image information including alt text"""
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
    """Extract all meta tags from the page"""
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
    """Extract structured data (JSON-LD) from the page"""
    if not soup:
        return []

    schemas = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            if script.string:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    schemas.append({
                        "type": data.get("@type", "Unknown"),
                        "found": True
                    })
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            schemas.append({
                                "type": item.get("@type", "Unknown"),
                                "found": True
                            })
        except Exception:
            continue

    return schemas


def _check_robots_meta(soup):
    """Check if page has robots meta tag"""
    if not soup:
        return False
    return soup.find("meta", attrs={"name": "robots"}) is not None


def _check_canonical(soup):
    """Check if page has canonical link"""
    if not soup:
        return False
    return soup.find("link", attrs={"rel": "canonical"}) is not None


def _check_sitemap(soup):
    """Check if page contains sitemap reference"""
    if not soup:
        return False
    return "sitemap" in str(soup).lower()


def _get_fallback_data(url):
    """Return fallback data structure when crawl fails"""
    return {
        "url": url,
        "title": "",
        "description": "",
        "ogTitle": "",
        "ogDescription": "",
        "ogImage": "",
        "language": "",
        "statusCode": 0,
        "sourceURL": url,
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
        "error": "Crawl failed - see logs for details"
    }