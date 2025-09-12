from __future__ import annotations
import tldextract
from urllib.parse import urlsplit, urlunsplit

def normalize_url(u: str) -> str:
    """Basic normalization: strip fragments, normalize scheme/host casing."""
    parts = urlsplit(u.strip())
    scheme = (parts.scheme or "http").lower()
    netloc = parts.netloc.lower()
    path = parts.path or "/"
    return urlunsplit((scheme, netloc, path, parts.query, ""))

def registrable_domain(u: str) -> str:
    ext = tldextract.extract(u)
    return ".".join([ext.domain, ext.suffix]) if ext.suffix else ext.domain
