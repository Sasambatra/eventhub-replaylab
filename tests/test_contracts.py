import pytest
from pydantic import ValidationError

from eventhub.contracts import CaseDefinition, Envelope, strict_json
from eventhub.security import signature, verify_signature
from eventhub.worker import decide, retry_after


@pytest.mark.parametrize(
    "source", [b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b"[" * 20 + b"0" + b"]" * 20]
)
def test_unsafe_json_rejected(source):
    with pytest.raises(ValueError):
        strict_json(source)


@pytest.mark.parametrize(
    "change", [{"revision": True}, {"revision": 0}, {"revision": -1}, {"status": "unknown"}]
)
def test_strict_snapshot(case, change):
    value = case["events"][0]
    value["data"].update(change)
    with pytest.raises(ValidationError):
        Envelope.model_validate(value)


def test_timezone_and_subject_required(case):
    value = case["events"][0]
    value["time"] = "2026-10-29T12:00:00"
    with pytest.raises(ValidationError):
        Envelope.model_validate(value)
    value["time"] = "2026-10-29T12:00:00Z"
    value["subject"] = "booking/other"
    with pytest.raises(ValidationError):
        Envelope.model_validate(value)


def test_case_cannot_contain_code_or_url(case):
    case["steps"][0]["url"] = "http://localhost/private"
    with pytest.raises(ValidationError):
        CaseDefinition.model_validate(case)


def test_signature_body_time_and_multiple_signatures():
    headers = signature(b"test-only-key", "delivery-1", b'{"ok":true}', timestamp=1000)
    assert verify_signature(b"test-only-key", headers, b'{"ok":true}', current_time=1001)
    assert not verify_signature(b"test-only-key", headers, b'{"ok":false}', current_time=1001)
    assert not verify_signature(b"test-only-key", headers, b'{"ok":true}', current_time=1400)
    headers["webhook-signature"] = "v1,invalid " + headers["webhook-signature"]
    assert verify_signature(b"test-only-key", headers, b'{"ok":true}', current_time=1001)


@pytest.mark.parametrize(
    "status,expected",
    [
        (200, "accepted"),
        (204, "accepted"),
        (301, "exhausted"),
        (401, "exhausted"),
        (409, "exhausted"),
        (500, "retry_wait"),
        (429, "retry_wait"),
    ],
)
def test_response_policy(config, status, expected):
    assert decide(status, "", 1, config)[0] == expected
    if status >= 400:
        assert decide(status, "", config.max_attempts, config)[0] == "exhausted"


def test_retry_after_honoured(config):
    from eventhub.db import now

    current = now()
    assert retry_after("3600", current) == 3600
    assert retry_after("bad", current) == 0
    state, at, _ = decide(429, "", 1, config, "3600", current)
    assert state == "retry_wait" and (at - current).total_seconds() >= 3600
