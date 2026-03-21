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
