"""
conftest.py — SmartStay Intelligence
Shared pytest fixtures.
"""

import pytest
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

# Load .env before anything imports the app — fixes POSTGRES_PORT=None
load_dotenv(ROOT / "backend" / ".env")

@pytest.fixture(scope="session")
def root():
    return ROOT

@pytest.fixture(scope="session")
def client():
    """FastAPI test client — imports app once for the whole session."""
    import sys
    sys.path.insert(0, str(ROOT / "backend"))
    from app.main import app
    from fastapi.testclient import TestClient
    # follow_redirects=True so redirect responses don't break tests
    return TestClient(app, follow_redirects=True)

# @pytest.fixture(scope="session")
# def auth_headers(client):
#     """
#     Log in and return cookie header for protected routes.
#     Auth uses httponly cookie (not Bearer), so we extract
#     the cookie and pass it manually on each request.
#     """
#     r = client.post("/api/v1/auth/login", json={
#         "identifier": "admin",   # your schema uses 'identifier', not 'username'
#         "password":   "admin123",
#     })
#     assert r.status_code == 200, f"Login failed: {r.status_code} — {r.text}"

#     # Token is set as httponly cookie — extract it for manual use in tests
#     token = r.cookies.get("access_token")
#     assert token, f"No access_token cookie in login response. Cookies: {dict(r.cookies)}"

#     return {"Cookie": f"access_token={token}"}