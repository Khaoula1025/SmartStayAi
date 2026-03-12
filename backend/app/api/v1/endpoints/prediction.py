from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.schemas.prediction import PredictionOut, ModelRunOut
from app.services import prediction_service

predictionRouter = APIRouter(prefix='/predictions', tags=['Predictions'])


@predictionRouter.get('/', response_model=List[PredictionOut])
def list_predictions(
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    rate_tier:    Optional[str]  = None,
    data_quality: Optional[str]  = None,
    limit:        int            = 365,
    db:           Session        = Depends(get_db),
    current_user: User           = Depends(get_current_user),
):
    """
    Return predictions from the latest promoted model run.

    - **rate_tier**: promotional | value | standard | high | premium
    - **data_quality**: high | medium | low (based on days ahead)
    """
    if not prediction_service.get_latest_run_id(db):
        raise HTTPException(404, 'No promoted model run found')
    return prediction_service.get_predictions(
        db, date_from, date_to, rate_tier, data_quality, limit
    )


@predictionRouter.get('/{target_date}', response_model=PredictionOut)
def get_prediction(
    target_date:  date,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """Full prediction detail for a single date including all adjustment columns."""
    pred = prediction_service.get_prediction_by_date(db, target_date)
    if not pred:
        raise HTTPException(404, f'No prediction found for {target_date}')
    return pred
