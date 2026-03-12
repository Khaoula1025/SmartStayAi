from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.schemas.analytics import AccuracySummary, ModelMetricsOut
from app.services import analytics_service, prediction_service

analyticsRouter = APIRouter(prefix='/analytics', tags=['Analytics'])


@analyticsRouter.get('/accuracy', response_model=AccuracySummary)
def get_accuracy(
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    db:           Session        = Depends(get_db),
    current_user: User           = Depends(get_current_user),
):
    """
    Prediction vs actual accuracy for all closed months.
    Returns per-date error rows plus summary stats and tier breakdown.
    """
    return analytics_service.get_accuracy_summary(db, date_from, date_to)


@analyticsRouter.get('/model/metrics', response_model=ModelMetricsOut)
def get_model_metrics(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """Latest promoted model — MAE, accuracy %, training rows."""
    run = prediction_service.get_model_metrics(db)
    if not run:
        raise HTTPException(404, 'No promoted model run found')
    return run


@analyticsRouter.get('/model/history', response_model=List[ModelMetricsOut])
def get_model_history(
    limit:        int     = 10,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """All model training runs, newest first. Shows accuracy trend over time."""
    return prediction_service.get_model_history(db, limit)
