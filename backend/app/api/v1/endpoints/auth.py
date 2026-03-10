 # /register, /login
from fastapi import APIRouter, Depends, HTTPException, Response, status
from app.schemas.auth import UserCreate,UserLogin
from app.models.user import User
from app.db.session import get_db
from sqlalchemy.orm import Session
from app.api.deps import signJwt, get_current_user
from app.core.security import hash_password, verify_password
from sqlalchemy import or_

authRouter = APIRouter(prefix="/auth")


@authRouter.post("/signUp")
async def create_user(user: UserCreate, db: Session = Depends(get_db)):

    found = db.query(User).filter(User.email == user.email).first()
    if found:
        raise HTTPException(status_code=409, detail='User Already exists')
    
    new_user = User(username=user.username,email=user.email, password=hash_password(user.password))
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return {"message": "Sign up successful"}


@authRouter.post('/login')
async def login(response: Response, user: UserLogin, db: Session = Depends(get_db)):

    found = db.query(User).filter(
        or_(
            User.email == user.identifier,
            User.username == user.identifier
        )
    ).first()
    
    if not found or not verify_password(user.password, found.password):
        raise HTTPException(status_code=401, detail='Invalid credentials')
    
    token = signJwt(found.id)
    response.set_cookie(
        key='access_token',
        value=token['access_token'],
        httponly=True,
        samesite='none',
        secure=True,
    )

    return {"message": "Login successful"}


@authRouter.post('/logout')
async def logout(response: Response):
    response.delete_cookie(
        key="access_token",
        httponly=True,
        samesite="none",
        secure=True          
    )
    return {'message': 'logout succesful', 'code': 200}


@authRouter.get('/verifyToken')
def verifyToken(current_user: str = Depends(get_current_user)):

    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )

    return {'message': 'success'}
