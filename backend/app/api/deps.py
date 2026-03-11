from fastapi import Depends, Cookie, HTTPException, status
from jose import jwt, JWTError
import time
import os
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from app.db.session import SessionLocal, get_db
from app.models.user import User

load_dotenv()

jwtSecret = os.getenv('secret')


def signJwt(user_id: int) -> dict:
    payload = {
        'sub':     str(user_id),
        'expires': time.time() + 8000,
    }
    token = jwt.encode(payload, jwtSecret, algorithm='HS256')
    return {
        'access_token': token,
        'token_type':   'Bearer',
    }


def cookie_key(access_token: str = Cookie(None)):
    if not access_token:
        raise HTTPException(status_code=401, detail='Not authenticated')
    return access_token


def get_current_user(
    token: str     = Depends(cookie_key),
    db:    Session = Depends(get_db),
) -> User:
    try:
        payload = jwt.decode(token, jwtSecret, algorithms=['HS256'])
        user_id = payload.get('sub')

        if user_id is None:
            raise HTTPException(status_code=401, detail='Invalid token')

        user = db.query(User).filter(User.id == int(user_id)).first()
        if not user:
            raise HTTPException(status_code=404, detail='User not found')

        return user

    except JWTError:
        raise HTTPException(status_code=401, detail='Could not validate credentials')


def require_admin(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != 'admin':
        raise HTTPException(status_code=403, detail='Admin role required')
    return current_user