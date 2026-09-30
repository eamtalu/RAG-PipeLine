"""Remove secrets from text before it leaves for a channel.

A failed M3 call's error text is the full request URL, and the handheld puts the encrypted M3 login
in it (`M3Credentials=%7B"Domain"…"Password"…"UserName"…%7D`). On tmp-live two alerts posted that to
Teams. Every text a card shows goes through `redact_secrets`; alerts are also stored redacted.

Pure and conservative: only a value under a secret-looking name is replaced, every other character
comes back untouched, so request ids, companies and users stay readable.
"""

from __future__ import annotations

import re

REDACTED = "<redacted>"

# A name is secret when it ends in one of these (case-insensitive): M3Credentials, Password,
# access_token, client_secret, api_key, apikey, …
_NAME = r"[\w.-]*?(?:credentials|password|passwd|token|secret|api[_-]?key)"

# key=value in a query string or form body: the value runs to the next separator. A value already
# redacted is left alone, so running the redaction twice changes nothing.
_QUERY = re.compile(rf"(?i)(?<![\w.-])({_NAME}=)(?!{REDACTED})[^&\s\"'<>]*")
# "key": {...} (a JSON object, not nested further), then "key": "value".
_JSON_OBJECT = re.compile(rf'(?i)("{_NAME}"\s*:\s*)\{{[^{{}}]*\}}')
_JSON_STRING = re.compile(rf'(?i)("{_NAME}"\s*:\s*")[^"]*(")')
# the same pair URL-encoded: %22key%22%3A%22value%22
_ENCODED = re.compile(rf"(?i)(%22{_NAME}%22%3A%22).*?(%22)")
_BEARER = re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+")


def redact_secrets(text: str | None) -> str | None:
    """`text` with every secret value replaced by `<redacted>`; None stays None."""
    if not text:
        return text
    out = _QUERY.sub(rf"\g<1>{REDACTED}", text)
    out = _JSON_OBJECT.sub(rf'\g<1>"{REDACTED}"', out)
    out = _JSON_STRING.sub(rf"\g<1>{REDACTED}\g<2>", out)
    out = _ENCODED.sub(rf"\g<1>{REDACTED}\g<2>", out)
    return _BEARER.sub(rf"\g<1>{REDACTED}", out)


def redact_value(value):
    """A copy of a stored payload with every string redacted; numbers, flags and None unchanged."""
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {k: redact_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_value(v) for v in value]
    return value
