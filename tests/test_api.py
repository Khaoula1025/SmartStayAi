import pytest


# ══════════════════════════════════════════════════════════════════════════════
# Auth helper
# ══════════════════════════════════════════════════════════════════════════════
@pytest.fixture(scope="session")
def auth_cookies(client):
    """Log in and return cookies for protected routes."""
    r = client.post("/api/v1/auth/login", json={
        "identifier": "test",
        "password": "12345678",
    })
    assert r.status_code == 200, f"Login failed: {r.status_code} — {r.text}"
    return r.cookies


# ══════════════════════════════════════════════════════════════════════════════
# Health  (public)
# ══════════════════════════════════════════════════════════════════════════════
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert "status" in data
    assert data["status"] in ("ok", "healthy")




# ══════════════════════════════════════════════════════════════════════════════
# Predictions / Forecast
# ══════════════════════════════════════════════════════════════════════════════
def test_predictions_returns_200(client, auth_cookies):
    r = client.get("/api/v1/predictions/", cookies=auth_cookies)
    assert r.status_code == 200

def test_predictions_not_empty(client, auth_cookies):
    r = client.get("/api/v1/predictions/", cookies=auth_cookies)
    assert len(r.json()) > 0

def test_predictions_have_required_fields(client, auth_cookies):
    r = client.get("/api/v1/predictions/", cookies=auth_cookies)
    first = r.json()[0]
    for field in ("date", "predicted_occ", "recommended_rate"):
        assert field in first, f"Missing field '{field}' in prediction row"


# ══════════════════════════════════════════════════════════════════════════════
# Analytics — Model metrics & history
# ══════════════════════════════════════════════════════════════════════════════
def test_model_metrics_returns_200(client, auth_cookies):
    r = client.get("/api/v1/analytics/model/metrics", cookies=auth_cookies)
    assert r.status_code == 200

def test_model_metrics_has_mae(client, auth_cookies):
    r = client.get("/api/v1/analytics/model/metrics", cookies=auth_cookies)
    data = r.json()
    assert "mae" in data or "mae_operational" in data or "cv_mae_operational" in data, \
    f"No MAE key found: {list(data.keys())}"