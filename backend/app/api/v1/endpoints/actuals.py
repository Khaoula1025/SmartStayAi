from datetime import date
from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.deps import get_current_user, require_admin
from app.models.user import User
from app.schemas.actual import ActualOut
from app.services import actual_service

actualsRouter = APIRouter(prefix='/actuals', tags=['Actuals'])

# Path to clean_occupancy.csv (two levels up from backend/)
_CLEAN_OCC_CSV = Path(__file__).resolve().parent.parent.parent.parent.parent \
                 / 'data' / 'processed' / 'clean_occupancy.csv'


@actualsRouter.get('/', response_model=List[ActualOut])
def list_actuals(
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    limit:        int            = 500,
    db:           Session        = Depends(get_db),
    current_user: User           = Depends(get_current_user),
):
    """
    Return historical actual occupancy for closed months.
    Used by the dashboard to draw the accuracy chart.
    """
    return actual_service.get_actuals(db, date_from, date_to, limit)


@actualsRouter.post('/load', status_code=200)
def load_actuals(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(require_admin),
):
    """
    Load (or refresh) actuals from clean_occupancy.csv into the database.
    **Admin only.** Safe to call multiple times — uses upsert.
    Call this after running Script 01 with new PMS data.
    """
    try:
        count = actual_service.load_actuals_from_csv(db, _CLEAN_OCC_CSV)
        return {'message': f'Loaded {count} actuals', 'source': str(_CLEAN_OCC_CSV)}
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, f'Failed to load actuals: {e}')
