from __future__ import annotations
import tldextract
from urllib.parse import urlsplit, urlunsplit
import re

_SCHEME_RE = re.compile(r'^[a-zA-Z][a-zA-Z0-9+.+-]*://')

def normalize_url(u: str) -> str:
    """Normalize: strip fragments, default scheme to https, lowercase scheme/host."""
    raw = (u or "").strip()
    if "#" in raw:
        raw = raw.split("#", 1)[0]

    if not _SCHEME_RE.match(raw):
        raw = "https://" + raw.lstrip("/")

    parts = urlsplit(raw)
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower()
    path   = parts.path or "/"
    return urlunsplit((scheme, netloc, path, parts.query, ""))

def registrable_domain(u: str) -> str:
    ext = tldextract.extract(u)
    return ".".join([ext.domain, ext.suffix]) if ext.suffix else ext.domain
