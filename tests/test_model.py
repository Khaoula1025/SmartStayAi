import pytest
import joblib
import numpy as np
from pathlib import Path

ROOT       = Path(__file__).resolve().parents[1]
MODELS_DIR = ROOT / "data" / "models"

FEATURE_COLS = [
    "month", "dow", "is_weekend", "is_high_season",
    "cs_occ", "cs_adr", "b_occ", "b_adr",
    "floor_price", "is_bank_holiday", "is_cultural_holiday",
    "is_local_event", "season",
]
N_FEATURES = len(FEATURE_COLS)   # 13


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def gbm():
    path = MODELS_DIR / "gbm_model.joblib"
    assert path.exists(), f"GBM model not found: {path}"
    return joblib.load(path)

@pytest.fixture(scope="module")
def rf():
    path = MODELS_DIR / "rf_model.joblib"
    assert path.exists(), f"RF model not found: {path}"
    return joblib.load(path)

@pytest.fixture(scope="module")
def sample_input():
    """One realistic prediction row (mid-July weekday, high season)."""
    return np.array([[
        7,      # month = July
        2,      # dow = Tuesday
        0,      # is_weekend = False
        1,      # is_high_season = True
        0.72,   # cs_occ (competitor occupancy)
        95.0,   # cs_adr (competitor rate)
        0.65,   # b_occ (budget occupancy)
        88.0,   # b_adr (budget rate)
        70.0,   # floor_price
        0,      # is_bank_holiday
        0,      # is_cultural_holiday
        0,      # is_local_event
        3,      # season (summer=3)
    ]])


# ── Model files exist ─────────────────────────────────────────────────────────

def test_gbm_file_exists():
    assert (MODELS_DIR / "gbm_model.joblib").exists()

def test_rf_file_exists():
    assert (MODELS_DIR / "rf_model.joblib").exists()


# ── Models load without error ─────────────────────────────────────────────────

def test_gbm_loads(gbm):
    assert gbm is not None

def test_rf_loads(rf):
    assert rf is not None


# ── Predictions are valid numbers ─────────────────────────────────────────────

def test_gbm_predict_returns_float(gbm, sample_input):
    pred = gbm.predict(sample_input)
    assert len(pred) == 1
    assert isinstance(float(pred[0]), float)
    assert not np.isnan(pred[0])

def test_rf_predict_returns_float(rf, sample_input):
    pred = rf.predict(sample_input)
    assert len(pred) == 1
    assert not np.isnan(pred[0])


# ── Predictions are in a sensible occupancy range [0, 1] ─────────────────────

def test_gbm_prediction_in_range(gbm, sample_input):
    pred = float(gbm.predict(sample_input)[0])
    assert 0.0 <= pred <= 1.0, f"GBM prediction out of range: {pred}"

def test_rf_prediction_in_range(rf, sample_input):
    pred = float(rf.predict(sample_input)[0])
    assert 0.0 <= pred <= 1.0, f"RF prediction out of range: {pred}"


# ── Model expects exactly 13 features ────────────────────────────────────────

def test_gbm_wrong_feature_count_raises(gbm):
    bad_input = np.array([[1, 2, 0]])   # only 3 features
    with pytest.raises(Exception):
        gbm.predict(bad_input)

def test_rf_wrong_feature_count_raises(rf):
    bad_input = np.array([[1, 2, 0]])
    with pytest.raises(Exception):
        rf.predict(bad_input)
