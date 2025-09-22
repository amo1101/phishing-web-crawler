from __future__ import annotations
import socket
import ssl
import concurrent.futures as cf
from typing import Tuple, Dict, List
from urllib.parse import urlsplit
import requests
import logging

log = logging.getLogger(__name__)

def resolve_host(host: str) -> bool:
    try:
        socket.getaddrinfo(host, None)
        return True
    except socket.gaierror:
        return False

def probe_url(url: str, timeout: int) -> Tuple[str, str]:
    """
    Returns (url, 'live'|'dead')
    """
    try:
        parts = urlsplit(url)
        if not parts.netloc:
            return url, "dead"
        if not resolve_host(parts.hostname):
            return url, "dead"
        # Try HEAD; fallback GET
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        log.debug("HEAD %s -> %s", url, r.status_code)
        if r.status_code >= 500:
            log.debug("HEAD %s -> network error", url)
            return url, "dead"
        return url, "live"
    except requests.RequestException:
        return url, "dead"

def classify_domains(urls: List[str], timeout: int, treat_4xx_as_live: bool, max_workers: int = 30) -> Dict[str, str]:
    """
    Returns {domain: 'live'|'dead'} based on the *best* observed status among its URLs.
    """
    # probe
    log.info("Liveness: probing %d URLs (timeout=%ss, workers=%d)", len(urls), timeout, max_workers)
    results: Dict[str, str] = {}
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(probe_url, u, timeout) for u in urls]
        for f in cf.as_completed(futs):
            url, status = f.result()
            results[url] = status

    # 4xx -> live (if configured)
    if treat_4xx_as_live:
        # requests.head already followed redirects and we only labeled 5xx/network as dead.
        pass

    # reduce to domain-level (live if any URL is live)
    from .normalize import registrable_domain
    domain_status: Dict[str, str] = {}
    for u, st in results.items():
        d = registrable_domain(u)
        prev = domain_status.get(d)
        if prev == "live":
            continue
        domain_status[d] = st if st == "dead" else "live"

    log.info("Liveness summary: live=%d dead=%d", sum(1 for s in domain_status.values() if s=="live"),
                                               sum(1 for s in domain_status.values() if s=="dead"))
    return domain_status
