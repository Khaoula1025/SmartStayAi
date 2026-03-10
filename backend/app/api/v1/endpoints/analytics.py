from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List
from app.schemas.prediction import AvgDurationByHour , PaymentAnalysis
from app.api.deps import get_current_user
from app.db.session import get_db



analyticsRouter = APIRouter()

@analyticsRouter.get("/avg-duration-by-hour", response_model=List[AvgDurationByHour])
def get_avg_duration_by_hour(
    credentials: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Durée moyenne par heure"""
    query = text("""
        WITH hourly_stats AS (
            SELECT 
                pickup_hour,
                trip_duration,
                COUNT(*) OVER (PARTITION BY pickup_hour) as trip_count
            FROM silver_table
            WHERE trip_duration > 0 AND trip_duration < 120
        )
        SELECT 
            pickup_hour,
            ROUND(AVG(trip_duration)::numeric, 2) as avg_duration,
            MAX(trip_count) as total_trips
        FROM hourly_stats
        GROUP BY pickup_hour
        ORDER BY pickup_hour
    """)
    
    result = db.execute(query)
    return [
        {"pickup_hour": r[0], "avg_duration": float(r[1]), "total_trips": r[2]}
        for r in result
    ]


@analyticsRouter.get("/payment-analysis", response_model=List[PaymentAnalysis])
def get_payment_analysis(
    credentials: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Analyse par type de paiement"""
    query = text("""
        SELECT 
            payment_type,
            COUNT(*) as total_trips,
            ROUND(AVG(trip_duration)::numeric, 2) as avg_duration,
            ROUND(AVG(fare_amount)::numeric, 2) as avg_fare
        FROM silver_table
        WHERE trip_duration > 0
        GROUP BY payment_type
        ORDER BY total_trips DESC
    """)
    
    result = db.execute(query)
    return [
        {
            "payment_type": r[0],
            "total_trips": r[1],
            "avg_duration": float(r[2]),
            "avg_fare": float(r[3])
        }
        for r in result
    ]