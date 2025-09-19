# tests/test_normalize.py
from crawler.normalize import normalize_url, registrable_domain


def test_normalize_url():
    u = "HTTP://Sub.Example.Co.NZ/path?q=1#frag"
    n = normalize_url(u)
    assert n == "http://sub.example.co.nz/path?q=1"


def test_registrable_domain_co_nz():
    assert registrable_domain("https://a.b.example.co.nz/") == "example.co.nz"
    assert registrable_domain("http://example.nz/") == "example.nz"
