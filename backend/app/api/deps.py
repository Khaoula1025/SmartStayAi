# Dependencies (get_current_user, get_db)
from fastapi import Depends , Cookie , HTTPException , status
from jose import jwt ,JWTError
import time
from app.db.session import sessionmaker,get_db
from app.models.user import User
import os 
from dotenv import load_dotenv

load_dotenv()

jwtSecret=os.getenv('secret')

def signJwt(user_id):
    payload={
        'sub':str(user_id),
         "expires": time.time() +8000
    }
    token = jwt.encode(payload,jwtSecret,algorithm='HS256')
    return {
        'access_token':token,
        'token_type':'Bearer'
    }

def cookie_key(access_token:str=Cookie(None)):
    if not access_token :
        raise HTTPException(status_code=401,detail='not authentified')
    return access_token

def get_current_user(
    token: str = Depends(cookie_key),
    db: sessionmaker = Depends(get_db)
):
    try:
        payload = jwt.decode(token, jwtSecret, algorithms=["HS256"])
        user_id = payload.get("sub")

        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")

        user = db.query(User).filter(User.id == int(user_id)).first()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return user

    except JWTError:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials"
        )
