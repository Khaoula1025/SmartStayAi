from fastapi import APIRouter, Depends , HTTPException
from fastapi.security import HTTPBearer
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.api.deps import get_current_user 
from app.schemas.prediction import PredictionResponse , TripFeatures
from datetime import datetime
from app.db.session import get_db
# from app.core import ml_model

predictionRouter = APIRouter()
security = HTTPBearer()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TaxiETAInference") \
    .getOrCreate()
MODEL_VERSION: str = "v1.0"
FEATURES = [
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'fare_amount',
    'tip_amount',
    'tolls_amount',
    'Airport_fee',
    'pickup_hour'
]

from pyspark.ml import PipelineModel

MODEL_PATH = "notebooks/models/GradientBoostedTrees_model"

model = PipelineModel.load(MODEL_PATH)

@predictionRouter.post("/predict", response_model=PredictionResponse)
def predict_eta(
    features: TripFeatures,
    user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    # Vérifier que le modèle est chargé
    if model is None: 
        raise HTTPException(status_code=503, detail="Modèle non disponible")
    
    try:
        # Créer DataFrame avec Spark
        data = [(
            features.passenger_count,
            features.trip_distance,
            features.RatecodeID,
            features.fare_amount,
            features.tip_amount,
            features.tolls_amount,
            features.Airport_fee,
            features.pickup_hour
        )]
        
        df = spark.createDataFrame(data, FEATURES)  
        # Prédiction
        result = model.transform(df)  # ← Utilise model importé
        duration = float(result.select("prediction").first()[0])
        # Logger dans PostgreSQL
        timestamp = datetime.now()
        log_query = text("""
           INSERT INTO eta_predictions (
    passenger_count,
    trip_distance,
    ratecode_id,
    fare_amount,
    tip_amount,
    tolls_amount,
    airport_fee,
    trip_duration,
    pickup_hour,
    predicted_duration,
    model_version,
    username
)
VALUES (
    :p, :d, :r, :f, :t, :toll, :air, :dur, :h, :pred, :v, :u
)
        """)
        
        db.execute(log_query, {
            "p": features.passenger_count,
            "d": features.trip_distance,
            "r": features.RatecodeID,
            "f": features.fare_amount,
            "t": features.tip_amount,
            "toll": features.tolls_amount,
            "air": features.Airport_fee,
            "dur": round(duration, 2),
            "h": features.pickup_hour,
            "pred": duration,
            "v": MODEL_VERSION,
            "ts": timestamp,
            "u": user.id
        })
        db.commit()
        
        return PredictionResponse(
            estimated_duration=round(duration, 2),
            model_version=MODEL_VERSION,
            timestamp=timestamp.isoformat()
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erreur prédiction: {str(e)}"
        )
