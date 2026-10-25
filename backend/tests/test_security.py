import os
import pathlib
import sys
from types import SimpleNamespace

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
os.environ.setdefault('ENABLE_IP_WHITELIST', 'true')

import app as backend  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[2]


def fake_request(headers, host='172.23.0.1'):
    return SimpleNamespace(headers=headers, client=SimpleNamespace(host=host))


def test_forwarded_headers_are_ignored():
    for header in ('X-Forwarded-For', 'X-Real-IP'):
        assert backend._client_ip(fake_request({header: '203.0.113.7'})) == '172.23.0.1'


def test_cloudflare_header_is_used():
    assert backend._client_ip(fake_request({'CF-Connecting-IP': '203.0.113.7'})) == '203.0.113.7'


def test_redis_keeps_nothing_on_disk():
    compose = (ROOT / 'docker-compose.yml').read_text()
    assert '--save ""' in compose and '--appendonly no' in compose and 'redis_data' not in compose
