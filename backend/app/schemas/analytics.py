from datetime import date, datetime
from typing import Optional, List
from pydantic import BaseModel


class AccuracyRow(BaseModel):
    date:          date
    actual_occ:    float
    predicted_occ: float
    abs_error:     float
    signed_error:  float
    within_ci:     bool
    rate_tier:     Optional[str] = None
    data_quality:  Optional[str] = None


class AccuracySummary(BaseModel):
    n_dates:           int
    mean_abs_error:    float
    mean_abs_error_pp: float
    within_ci_pct:     float
    by_tier:           dict
    rows:              List[AccuracyRow]


class ModelMetricsOut(BaseModel):
    run_id:            str
    trained_at:        datetime
    model_type:        Optional[str]   = None
    n_training_rows:   int
    n_prediction_rows: int
    mae_operational:   Optional[float] = None
    occ_accuracy_pct:  Optional[float] = None
    stage2_regime:     Optional[str]   = None
    promoted:          bool

    class Config:
        from_attributes = True
