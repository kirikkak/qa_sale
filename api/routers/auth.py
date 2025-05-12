from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import string
import random
import os
from datetime import datetime, timedelta
from passlib.context import CryptContext
import jwt

from api.dependencies import get_db, get_current_user
from models import User
from api.schemas import RegisterRequest, LoginRequest, TokenResponse, BalanceResponse

# JWT configuration
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "supersecretkey")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
router = APIRouter()


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


@router.post("/register", status_code=201)
def register(
    req: RegisterRequest,
    db: Session = Depends(get_db)
):
    existing = db.query(User).filter(User.username == req.username).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already registered"
        )

    alphabet = string.ascii_letters + string.digits
    raw_password = ''.join(random.choice(alphabet) for _ in range(12))
    hashed_pw = get_password_hash(raw_password)

    user = User(username=req.username, hashed_password=hashed_pw)
    db.add(user)
    db.commit()
    db.refresh(user)

    print(f"New user registered -> username: {req.username}, password: {raw_password}")

    return {"message": "User registered successfully"}


@router.post("/login", response_model=TokenResponse)
def login(
    req: LoginRequest,
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.username == req.username).first()
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )

    access_token = create_access_token({"sub": str(user.id)})
    return TokenResponse(access_token=access_token, token_type="bearer")

@router.get(
    "/balance",
    response_model=BalanceResponse,
    summary="Получить баланс минут текущего пользователя"
)
def get_balance(current_user=Depends(get_current_user)):
    """
    Возвращает, сколько минут осталось на балансе у залогиненного пользователя.
    """
    return BalanceResponse(balance_minutes=current_user.balance_minutes)
