# tests/test_wayback.py
from crawler.wayback import cdx_latest_snapshots_for_url


def test_cdx_latest_snapshots_for_url(fake_requests):
    base = "https://web.archive.org/cdx/search/cdx"
    params = "?url=https%3A%2F%2Fexample.nz%2Fprofile.php%3Fid%3D123&matchType=exact&output=json&fl=timestamp%2Coriginal%2Cstatuscode%2Cdigest&filter=statuscode%3A200&collapse=digest"
    payload = [
        ["20250102030405", "https://example.nz/profile.php?id=123", "200", "AAA"],
        ["20241211121314", "https://example.nz/profile.php?id=123", "200", "BBB"],
        ["20231201000000", "https://example.nz/profile.php?id=123", "200", "CCC"],
    ]
    fake_requests.register_get(base + params, fake_requests.FakeResp(status=200, json_data=payload))
    stamps = cdx_latest_snapshots_for_url("https://example.nz/profile.php?id=123", 2, base, {
        "matchType": "exact", "output": "json",
        "fl": "timestamp,original,statuscode,digest",
        "filter": "statuscode:200", "collapse": "digest",
    }, rps=100.0)
    assert stamps == ["20250102030405", "20241211121314"]
