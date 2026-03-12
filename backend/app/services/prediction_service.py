from datetime import date
from typing import Optional, List

from sqlalchemy.orm import Session

from app.models.prediction import ModelRun, Prediction

HOTEL = 'hickstead'


def get_latest_run_id(db: Session) -> Optional[str]:
    run = (
        db.query(ModelRun)
        .filter(ModelRun.promoted == True, ModelRun.hotel == HOTEL)
        .order_by(ModelRun.trained_at.desc())
        .first()
    )
    return str(run.run_id) if run else None


def get_predictions(
    db:           Session,
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    rate_tier:    Optional[str]  = None,
    data_quality: Optional[str]  = None,
    limit:        int            = 365,
) -> List[Prediction]:
    run_id = get_latest_run_id(db)
    if not run_id:
        return []

    q = (
        db.query(Prediction)
        .filter(Prediction.hotel  == HOTEL)
        .filter(Prediction.run_id == run_id)
    )
    if date_from:    q = q.filter(Prediction.date >= date_from)
    if date_to:      q = q.filter(Prediction.date <= date_to)
    if rate_tier:    q = q.filter(Prediction.rate_tier == rate_tier)
    if data_quality: q = q.filter(Prediction.data_quality == data_quality)

    return q.order_by(Prediction.date).limit(limit).all()


def get_prediction_by_date(db: Session, target_date: date) -> Optional[Prediction]:
    run_id = get_latest_run_id(db)
    if not run_id:
        return None
    return (
        db.query(Prediction)
        .filter(
            Prediction.hotel  == HOTEL,
            Prediction.run_id == run_id,
            Prediction.date   == target_date,
        )
        .first()
    )


def get_model_metrics(db: Session) -> Optional[ModelRun]:
    return (
        db.query(ModelRun)
        .filter(ModelRun.promoted == True, ModelRun.hotel == HOTEL)
        .order_by(ModelRun.trained_at.desc())
        .first()
    )


def get_model_history(db: Session, limit: int = 10) -> List[ModelRun]:
    return (
        db.query(ModelRun)
        .filter(ModelRun.hotel == HOTEL)
        .order_by(ModelRun.trained_at.desc())
        .limit(limit)
        .all()
    )
