"""Trusted-header (SSO gateway) authentication."""

import os
import unittest
from datetime import datetime, timezone
from unittest import mock
from uuid import uuid4

from config import Config
from database import UserDB, UserSessionDB
from security.auth import AuthError, AuthManager
from security.trusted_header import (
    TRUSTED_HEADER_PROVIDER,
    TrustedIdentity,
    extract_trusted_identity,
)
from services.auth_service import AuthService
from tests.base import DatabaseTestCase

SECRET = "gateway-secret"


def make_config(**overrides) -> Config:
    env = {
        "AUTH_TRUSTED_HEADER_ENABLED": "true",
        "AUTH_TRUSTED_HEADER_SECRET": SECRET,
        "SESSION_SECRET_KEY": "test-session-secret",
        "TOKEN_SECRET_KEY": "test-token-secret",
    }
    env.update(overrides)
    with mock.patch.dict(os.environ, env, clear=False):
        config = Config()
    # Class-body default, evaluated at import time; flip it on the instance.
    config.auth_enabled = True
    return config


def headers(**extra) -> dict:
    base = {
        "X-Neuroflow-Proxy-Secret": SECRET,
        "X-authentik-username": "alice",
        "X-authentik-email": "Alice@Example.org",
        "X-authentik-name": "Alice Example",
    }
    base.update(extra)
    return {k: v for k, v in base.items() if v is not None}


class ExtractTrustedIdentityTests(unittest.TestCase):
    def test_disabled_mode_is_rejected(self):
        config = make_config(AUTH_TRUSTED_HEADER_ENABLED="false")
        with self.assertRaises(AuthError):
            extract_trusted_identity(headers(), config)

    def test_missing_secret_configuration_is_rejected(self):
        config = make_config(AUTH_TRUSTED_HEADER_SECRET="")
        self.assertFalse(config.trusted_header_ready)
        with self.assertRaises(AuthError):
            extract_trusted_identity(headers(), config)

    def test_missing_secret_header_is_rejected(self):
        config = make_config()
        h = headers()
        del h["X-Neuroflow-Proxy-Secret"]
        with self.assertRaises(AuthError):
            extract_trusted_identity(h, config)

    def test_wrong_secret_is_rejected(self):
        config = make_config()
        with self.assertRaises(AuthError):
            extract_trusted_identity(headers(**{"X-Neuroflow-Proxy-Secret": "nope"}), config)

    def test_missing_username_is_rejected(self):
        config = make_config()
        with self.assertRaises(AuthError):
            extract_trusted_identity(headers(**{"X-authentik-username": "  "}), config)

    def test_identity_is_normalised(self):
        config = make_config()
        identity = extract_trusted_identity(headers(), config)
        self.assertEqual(identity, TrustedIdentity("alice", "alice@example.org", "Alice Example"))

    def test_missing_email_gets_synthetic_address(self):
        config = make_config(AUTH_TRUSTED_HEADER_EMAIL_DOMAIN="lab.invalid")
        identity = extract_trusted_identity(headers(**{"X-authentik-email": None}), config)
        self.assertEqual(identity.email, "alice@lab.invalid")

    def test_header_names_are_configurable(self):
        config = make_config(
            AUTH_TRUSTED_HEADER_SECRET_HEADER="X-Proxy-Token",
            AUTH_TRUSTED_HEADER_USERNAME="X-Forwarded-User",
            AUTH_TRUSTED_HEADER_EMAIL="X-Forwarded-Email",
        )
        identity = extract_trusted_identity(
            {"X-Proxy-Token": SECRET, "X-Forwarded-User": "bob", "X-Forwarded-Email": "bob@example.org"},
            config,
        )
        self.assertEqual(identity.username, "bob")
        self.assertEqual(identity.email, "bob@example.org")

    def test_ready_requires_auth_enabled(self):
        config = make_config()
        self.assertTrue(config.trusted_header_ready)
        config.auth_enabled = False
        self.assertFalse(config.trusted_header_ready)


class TrustedHeaderLoginTests(DatabaseTestCase):
    def setUp(self):
        super().setUp()
        self.config = make_config()
        self.auth_manager = AuthManager(self.config)
        self.service = AuthService(self.session, self.auth_manager)

    def test_login_creates_user_and_session(self):
        identity = TrustedIdentity("alice", "alice@example.org", "Alice Example")
        result = self.service.trusted_header_login(identity)

        user = self.session.query(UserDB).filter(UserDB.email == "alice@example.org").one()
        self.assertEqual(user.provider, TRUSTED_HEADER_PROVIDER)
        self.assertEqual(user.provider_account_id, "alice")
        self.assertEqual(user.name, "Alice Example")

        session = self.session.query(UserSessionDB).filter(
            UserSessionDB.session_id == result.session.session_id
        ).one()
        self.assertEqual(session.user_id, user.id)
        self.assertEqual(session.auth_provider, TRUSTED_HEADER_PROVIDER)

        authenticated = self.service.authenticate_access_token(result.access_token)
        self.assertEqual(authenticated.id, user.id)
        self.assertEqual(authenticated.email, "alice@example.org")

    def test_login_attaches_to_requested_session(self):
        identity = TrustedIdentity("alice", "alice@example.org", None)
        result = self.service.trusted_header_login(identity, session_id="browser-session")
        self.assertEqual(result.session.session_id, "browser-session")

    def test_login_reuses_account_with_same_email_from_other_provider(self):
        now = datetime.now(timezone.utc)
        existing = UserDB(
            id=str(uuid4()),
            email="alice@example.org",
            name="alice",
            provider="basic",
            provider_account_id="alice@example.org",
            created_at=now,
            updated_at=now,
        )
        self.session.add(existing)
        self.session.commit()

        identity = TrustedIdentity("alice", "alice@example.org", "Alice Example")
        result = self.service.trusted_header_login(identity)

        self.assertEqual(result.user.id, existing.id)
        self.assertEqual(self.session.query(UserDB).count(), 1)
        self.assertEqual(result.user.name, "Alice Example")
        self.assertEqual(result.session.auth_provider, TRUSTED_HEADER_PROVIDER)

    def test_login_respects_email_allow_list(self):
        config = make_config(ALLOWED_EMAIL_PATTERNS="*@example.org")
        service = AuthService(self.session, AuthManager(config))
        service.trusted_header_login(TrustedIdentity("alice", "alice@example.org", None))
        with self.assertRaises(AuthError):
            service.trusted_header_login(TrustedIdentity("mallory", "mallory@elsewhere.net", None))


if __name__ == "__main__":
    unittest.main()
