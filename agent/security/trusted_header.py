"""Trusted-header authentication: identity asserted by a reverse proxy.

A single sign-on gateway (for example nginx with Authentik forward auth) logs
the browser in, then forwards each request to Kanchi with the user's identity
in headers such as ``X-authentik-username``. Headers are trivial to forge, so
the gateway also sends a shared secret in a second header; Kanchi only trusts
the identity when that secret matches ``AUTH_TRUSTED_HEADER_SECRET``.

The result of :func:`extract_trusted_identity` feeds
``AuthService.trusted_header_login``, which issues Kanchi's normal access and
refresh tokens. Everything downstream (bearer tokens, WebSocket auth, refresh)
is unchanged.
"""

import hmac
from dataclasses import dataclass
from typing import Mapping, Optional

from config import Config

from .auth import AuthError

# Value stored in ``users.provider`` / ``user_sessions.auth_provider`` for
# accounts created through the gateway.
TRUSTED_HEADER_PROVIDER = "header"


@dataclass(frozen=True)
class TrustedIdentity:
    """Identity the gateway vouched for."""

    username: str
    email: str
    name: Optional[str] = None


def extract_trusted_identity(headers: Mapping[str, str], config: Config) -> TrustedIdentity:
    """Validate the gateway secret and read the forwarded identity.

    ``headers`` is any mapping with header-name keys; Starlette's
    ``request.headers`` (case-insensitive) is the intended input.

    Raises :class:`AuthError` when the mode is off, no secret is configured,
    the secret does not match, or no username was forwarded.
    """
    if not config.auth_trusted_header_enabled:
        raise AuthError("Trusted header authentication disabled")

    expected = config.auth_trusted_header_secret or ""
    if not expected:
        raise AuthError("Trusted header authentication has no secret configured")

    presented = headers.get(config.auth_trusted_header_secret_header) or ""
    if not hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8")):
        raise AuthError("Request did not come through the trusted proxy")

    username = (headers.get(config.auth_trusted_header_username) or "").strip()
    if not username:
        raise AuthError("Trusted proxy did not forward a username")

    email = (headers.get(config.auth_trusted_header_email) or "").strip().lower()
    if not email:
        # Kanchi keys accounts by email; synthesise a stable one when the
        # identity provider has none on file.
        email = f"{username.lower()}@{config.auth_trusted_header_email_domain}"

    name = (headers.get(config.auth_trusted_header_name) or "").strip() or None

    return TrustedIdentity(username=username, email=email, name=name)
