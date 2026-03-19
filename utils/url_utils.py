"""
Utilities for URL validation and access.

Provides functions for validating URLs and checking their availability.
"""

import re
from typing import Dict, Optional, Tuple
import requests

# Headers to mimic a real browser and avoid abuse/filter blocks.
# Includes Client Hints (Sec-CH-UA*) that Chrome sends; some WAFs check for these.
BROWSER_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def is_valid_url(url: str) -> bool:
    """
    Validate that URL is a valid HTTP/HTTPS URL.
    
    Args:
        url: URL to validate
        
    Returns:
        True if URL is valid, False otherwise
        
    Example:
        >>> is_valid_url("https://example.com")
        True
        >>> is_valid_url("not-a-url")
        False
    """
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    return url.startswith('http://') or url.startswith('https://')


def access_url(url: str, timeout: int = 30) -> Tuple[bool, str]:
    """
    Access a URL and return status information.
    
    Args:
        url: URL to access
        timeout: Request timeout in seconds
        
    Returns:
        Tuple of (success: bool, status_message: str)
        
    Example:
        >>> success, status = access_url("https://example.com")
        >>> success
        True
        >>> status
        'Success'
    """
    try:
        response = requests.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers=BROWSER_HEADERS,
        )
        if response.status_code == 200:
            return True, "Success"
        else:
            return False, f"HTTP {response.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except requests.exceptions.ConnectionError:
        return False, "Connection Error"
    except requests.exceptions.TooManyRedirects:
        return False, "Too Many Redirects"
    except requests.exceptions.RequestException as e:
        return False, f"Error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected Error: {str(e)}"


def infer_file_type(url: str, content_type: Optional[str] = None) -> str:
    """
    Infer file type/extension from URL path or Content-Type header.

    Prefers URL path extension when present; otherwise maps common Content-Type
    values to extensions (e.g. text/csv -> csv, application/json -> json).

    Args:
        url: Resource URL (may have path with extension)
        content_type: Optional Content-Type header value (e.g. "text/csv")

    Returns:
        Lowercase file type string (e.g. "csv", "json", "html") or "unknown".
    """
    from urllib.parse import urlparse, unquote

    parsed = urlparse(unquote(url))
    path = parsed.path.rstrip("/")
    if "." in path.split("/")[-1]:
        ext = path.split(".")[-1].lower()
        if ext and len(ext) <= 5 and ext.isalnum():
            return ext

    if content_type:
        ct = content_type.lower().strip()
        mapping = {
            "text/csv": "csv",
            "application/json": "json",
            "application/xml": "xml",
            "text/xml": "xml",
            "text/html": "html",
            "application/rdf+xml": "rdf",
            "application/zip": "zip",
            "application/x-zip-compressed": "zip",
            "application/vnd.ms-excel": "xls",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
            "text/plain": "txt",
        }
        return mapping.get(ct, ct.split("/")[-1] if "/" in ct else "unknown")
    return "unknown"


# Phrases in HTML body that indicate a "page not found" error page (case-insensitive).
_HTML_NOT_FOUND_PHRASES = (
    "page not found",
    "the page you requested could not be found",
    "sorry, the page you requested could not be found",
)

# Real 404 pages are usually short. Large pages (e.g. data catalog dataset views)
# that incidentally contain the phrase in templates/scripts should not be flagged.
_HTML_NOT_FOUND_MAX_BODY_LEN = 15000


def _html_body_looks_like_not_found(body: str) -> bool:
    """Return True if HTML body contains not-found error phrases and is short enough."""
    if not body or len(body) > _HTML_NOT_FOUND_MAX_BODY_LEN:
        return False
    lower = body.lower()
    return any(phrase in lower for phrase in _HTML_NOT_FOUND_PHRASES)


def body_looks_like_not_found(body: str) -> bool:
    """
    Return True if HTML body contains not-found error phrases (logical 404).

    Uses the same phrases as internal 404 detection (e.g. "page not found",
    "the page you requested could not be found"). Intended for callers that
    fetch full page body and need to classify logical 404s.

    Args:
        body: Raw HTML body text to check.

    Returns:
        True if body looks like a not-found error page, False otherwise.
    """
    return _html_body_looks_like_not_found(body)


def fetch_url_head(
    url: str, timeout: int = 30
) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Perform a HEAD request and return status code, Content-Type, and error message.

    Treats as 404: HTTP 404, connection errors ("Failed to establish a new connection"),
    and 200 responses with HTML body containing "page not found" or similar phrases.

    Args:
        url: URL to fetch
        timeout: Request timeout in seconds

    Returns:
        Tuple of (status_code: int, content_type: Optional[str], error_message: Optional[str]).
        On success: (status_code, content_type, None).
        On HTTP 404 or not-found-like: (404, None, None) or (404, None, error_msg).
        On other exception: (-1, None, str(cause)).
    """
    try:
        response = requests.head(
            url,
            timeout=timeout,
            allow_redirects=True,
            headers=BROWSER_HEADERS,
        )
        content_type = response.headers.get("Content-Type")
        if content_type and ";" in content_type:
            content_type = content_type.split(";")[0].strip()

        # If 200 with HTML, fetch body and check for "page not found" style content
        if response.status_code == 200 and content_type and "text/html" in content_type.lower():
            get_resp = requests.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                stream=True,
                headers=BROWSER_HEADERS,
            )
            get_resp.raw.decode_content = True
            chunk = get_resp.raw.read(16384)
            try:
                text = chunk.decode("utf-8", errors="ignore")
            except Exception:
                text = chunk.decode("latin-1", errors="ignore")
            if _html_body_looks_like_not_found(text):
                return 404, None, None

        return response.status_code, content_type, None
    except Exception as exc:
        cause = exc.__cause__ if exc.__cause__ is not None else exc
        err_str = str(cause)
        if "Failed to establish a new connection" in err_str:
            return 404, None, err_str
        return -1, None, err_str


# Binary magic bytes: do not decode as text even if Content-Type says text.
_BINARY_MAGIC_PREFIXES = (
    b"\x1f\x8b",  # gzip
    b"%PDF",
    b"\x89PNG",
    b"PK\x03\x04",  # ZIP
    b"\xff\xd8\xff",  # JPEG
)


def _raw_looks_binary(raw: bytes) -> bool:
    """Return True if raw content looks like binary (e.g. compressed or PDF)."""
    if len(raw) < 2:
        return False
    return any(raw.startswith(prefix) for prefix in _BINARY_MAGIC_PREFIXES)


def _decoded_looks_like_garbage(text: str) -> bool:
    """Return True if decoded text has too few printable chars (likely binary decoded as text)."""
    if not text or len(text) < 10:
        return False
    # Use Unicode notion of printable so UTF-8 HTML (curly quotes, em-dash, etc.) passes
    printable = sum(1 for c in text if c.isprintable() or c in "\t\n\r")
    return (printable / len(text)) < 0.7


def _is_text_content_type(content_type: Optional[str]) -> bool:
    """Return True if content type is typically decodable as text (e.g. HTML, JSON, XML)."""
    if not content_type:
        return False
    ct = content_type.lower().strip()
    if ct.startswith("text/"):
        return True
    if ct in (
        "application/xml",
        "application/json",
        "application/javascript",
        "application/xhtml+xml",
    ):
        return True
    return False


def is_displayable_content_type(content_type: Optional[str]) -> bool:
    """
    Return True if we should show the response body as text in an iframe (not binary).

    Like _is_text_content_type but excludes XML (application/xml, text/xml) so XML
    is offered as download instead of displayed. Used by Interactive Collector to
    decide when to show the download button for non-HTML links.
    """
    if not content_type:
        return True
    ct = content_type.lower().strip()
    if ct in ("application/xml", "text/xml"):
        return False
    if ct.startswith("text/"):
        return True
    if ct in (
        "application/json",
        "application/javascript",
        "application/xhtml+xml",
    ):
        return True
    return False


def body_looks_like_xml(body: Optional[str | bytes]) -> bool:
    """Return True if body starts with XML declaration (e.g. when Content-Type is wrong)."""
    if body is None:
        return False
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    s = (body or "").strip()
    return len(s) >= 5 and s[:5].lower() == "<?xml"


def body_looks_like_html(body: Optional[str | bytes]) -> bool:
    """
    Return True if the body looks like an HTML document (not data XML).

    Pages like NCBI BioSample may be served as application/xml but are actually HTML.
    We treat as HTML if we see <!DOCTYPE html or <html in the first 8KB.
    """
    if body is None:
        return False
    if isinstance(body, bytes):
        body = body.decode("utf-8", errors="replace")
    s = (body or "").strip()
    if len(s) < 4:
        return False
    head = s[:8192].lower()
    return "<!doctype html" in head or "<html" in head


def is_non_html_response(
    content_type: Optional[str],
    body: Optional[str | bytes],
    raw_bytes: Optional[bytes] = None,
) -> bool:
    """
    Return True if the response should be treated as non-HTML (offer download, not display).

    Uses the same logic as the Interactive Collector: magic bytes, Content-Type,
    and body sniffing (XML vs HTML). Reuse this to decide when to show the download
    button for links (PDF, CSV, ZIP, XML, etc.) instead of displaying in an iframe.

    Args:
        content_type: Response Content-Type header (or None).
        body: Decoded response body (text or empty for binary). From fetch_page_body.
        raw_bytes: Optional raw response bytes. When provided, magic-byte detection
            (PDF, ZIP, PNG, JPEG, gzip) overrides Content-Type.

    Returns:
        True if we should offer download (binary/XML/non-displayable).
    """
    if raw_bytes is not None and len(raw_bytes) >= 2 and _raw_looks_binary(raw_bytes):
        return True
    if body_looks_like_html(body):
        return False
    if content_type and not is_displayable_content_type(content_type):
        return True
    if body_looks_like_xml(body):
        return True
    return False


def _is_aws_waf_challenge(status_code: int, body: str) -> bool:
    """
    Return True if the response looks like an AWS WAF JavaScript challenge page.

    catalog.data.gov and other data.gov domains use AWS WAF and return HTTP 202
    with a challenge page that requires JavaScript. Python requests cannot pass
    the challenge; a real browser (Playwright) can. Some variants return 200
    with "Human Verification" in the title.
    """
    if status_code not in (200, 202):
        return False
    if not body or len(body) < 100:
        return False
    lower = body.lower()
    # Common WAF challenge indicators
    if (
        "awswaf" in lower
        or "challenge.js" in lower
        or "challenge-container" in lower
        or "human verification" in lower
    ):
        return True
    # noscript block with JS requirement (various phrasings)
    if "noscript" in lower and (
        "javascript is disabled" in lower
        or "javascript is not enabled" in lower
        or "enable javascript" in lower
        or "javascript must be enabled" in lower
    ):
        return True
    return False


def is_waf_challenge(status_code: int, body: str) -> bool:
    """
    Public check: return True if the response is an AWS WAF challenge page.

    Callers can use this to show a friendly message instead of the raw challenge HTML.
    """
    return _is_aws_waf_challenge(status_code, body)


def _fetch_page_body_with_playwright(
    url: str, timeout: int = 60
) -> Tuple[int, str, Optional[str], bool]:
    """
    Fetch a page using Playwright to bypass AWS WAF bot challenges.

    Returns same tuple as fetch_page_body. Used as fallback when requests.get
    returns a WAF challenge (HTTP 202 with challenge page).
    Set DRP_FETCH_HEADED=1 to use a visible browser (may help pass WAF in some environments).
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return -1, "", None, False

    import os
    headless = False #(os.environ.get("DRP_FETCH_HEADED") or "").strip().lower() not in ("1", "true", "yes", "on")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            try:
                page = browser.new_page()
                page.set_default_timeout(timeout * 1000)
                page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
                # Allow WAF challenge to complete (it may trigger reload)
                page.wait_for_load_state("load", timeout=(timeout - 10) * 1000)
                page.wait_for_timeout(2000)  # Extra for post-load content
                body = page.content()
            finally:
                browser.close()

        if not body or len(body) < 100:
            return -1, body or "", "text/html", False
        # Check if we still got a challenge page (e.g. headless detected)
        if _is_aws_waf_challenge(200, body):
            return 202, body, "text/html", False
        return 200, body, "text/html", False
    except Exception:
        return -1, "", None, False


def _http_get(url: str, headers: Dict[str, str], timeout: int) -> "requests.Response":
    """
    Perform GET with optional curl_cffi for Chrome TLS impersonation.

    If curl_cffi is installed, uses it first (impersonate="chrome") to mimic
    Chrome's TLS fingerprint; may bypass AWS WAF that blocks plain requests.
    Falls back to requests otherwise.
    """
    try:
        from curl_cffi import requests as curl_requests
        return curl_requests.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
            impersonate="chrome120",
        )
    except ImportError:
        pass
    return requests.get(
        url,
        headers=headers,
        timeout=timeout,
        allow_redirects=True,
    )


def fetch_page_body(
    url: str, timeout: int = 30
) -> Tuple[int, str, Optional[str], bool]:
    """
    Fetch a URL with GET and return status, body, content-type, and logical-404 flag.

    Uses BROWSER_HEADERS. Connection failures and HTTP 404 are returned as
    status 404 with is_logical_404 False. A 200 response with HTML body
    that contains "page not found" style content is returned as status 404
    with is_logical_404 True so callers can distinguish logical 404s.

    Only decodes the response body when Content-Type indicates text (e.g. text/html,
    application/json). For binary types (PDF, images, etc.) body is returned empty
    to avoid decoding garbage.

    Args:
        url: URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        Tuple of (status_code, body, content_type, is_logical_404).
        - status_code: HTTP status or 404 for connection/not-found, -1 for other errors.
        - body: Response body text (empty on connection/other errors or for binary content).
        - content_type: Parsed Content-Type (None if missing or on error).
        - is_logical_404: True only when status is 404 due to body content (200 + not-found phrases).
    """
    # Prefer gzip/deflate only so response is always decompressed (no Brotli)
    headers = {**BROWSER_HEADERS, "Accept-Encoding": "gzip, deflate"}
    try:
        response = _http_get(url, headers, timeout)
        content_type = response.headers.get("Content-Type")
        if content_type and ";" in content_type:
            content_type = content_type.split(";")[0].strip()

        raw = response.content
        if not _is_text_content_type(content_type) or _raw_looks_binary(raw):
            body = ""
        else:
            body = raw.decode("utf-8", errors="replace")
            if _decoded_looks_like_garbage(body):
                body = ""

        if response.status_code == 404:
            return 404, body, content_type, False
        if response.status_code == 200 and content_type and "text/html" in content_type.lower():
            if _html_body_looks_like_not_found(body):
                return 404, body, content_type, True
        # catalog.data.gov uses AWS WAF; requests returns 202 with a challenge page.
        # Fall back to Playwright so a real browser can pass the challenge.
        if _is_aws_waf_challenge(response.status_code, body):
            pw_status, pw_body, pw_ct, pw_404 = _fetch_page_body_with_playwright(url, timeout)
            if pw_status == 200 and pw_body:
                return pw_status, pw_body, pw_ct or "text/html", pw_404
            # If Playwright also failed, return original response
        return response.status_code, body, content_type, False
    except Exception as exc:
        cause = exc.__cause__ if exc.__cause__ is not None else exc
        err_str = str(cause)
        if "Failed to establish a new connection" in err_str:
            return 404, "", None, False
        return -1, "", None, False


def resolve_catalog_resource_url(catalog_url: str, timeout: int = 30) -> Optional[str]:
    """
    Resolve a catalog.data.gov resource page URL to the actual download URL.

    Fetches the HTML page and reads the <a id="res_url"> element's href, which
    points to the real file (S3, data.gov redirect, etc.). Does not use a browser;
    uses the same fetch as fetch_page_body. Returns None if the page is 404 or
    logical 404, or if #res_url is missing.

    Args:
        catalog_url: URL of a catalog.data.gov resource page (e.g. .../dataset/.../resource/...).
        timeout: Request timeout in seconds.

    Returns:
        The resolved download URL, or None.
    """
    if not catalog_url.startswith("https://catalog.data.gov"):
        return None
    status_code, body, content_type, is_logical_404 = fetch_page_body(catalog_url, timeout=timeout)
    if status_code == 404 or is_logical_404:
        return None
    if not body or not content_type or "text/html" not in content_type.lower():
        return None
    # Match <a id="res_url" href="..."> or id then href in either order
    match = re.search(
        r'<a\s+[^>]*id\s*=\s*["\']res_url["\'][^>]*href\s*=\s*["\']([^"\']+)["\']',
        body, re.I | re.DOTALL
    )
    if not match:
        match = re.search(
            r'<a\s+[^>]*href\s*=\s*["\']([^"\']+)["\'][^>]*id\s*=\s*["\']res_url["\']',
            body, re.I | re.DOTALL
        )
    if match:
        return match.group(1).strip()
    return None
