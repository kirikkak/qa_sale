import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from kafka import KafkaProducer, errors as kafka_errors
from elasticsearch import Elasticsearch
from fastapi import HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer
import jwt
from models import Checklist as ChecklistModel


# Загрузка переменных окружения
load_dotenv()

# Настройка базы данных
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Зависимость FastAPI для получения сессии SQLAlchemy
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# JWT настройка
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "supersecretkey")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

# OAuth2 схема
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# Импорт модели User
from models import User

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> User:
    """
    Зависимость для получения текущего пользователя из JWT токена
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    user = db.query(User).filter(User.id == int(user_id)).first()
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Inactive or non-existent user",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user

# Kafka Bootstrap Servers
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

producer = None

def get_kafka():
    """
    Зависимость для получения KafkaProducer.
    Создаётся при первом запросе.
    """
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        except kafka_errors.NoBrokersAvailable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Kafka brokers unavailable"
            )
    return producer

# Проверка активности чеклиста
def get_active_checklist(checklist_id: int, db: Session, current_user) -> ChecklistModel:
    checklist = (
        db.query(ChecklistModel)
        .filter(
            ChecklistModel.id == checklist_id,
            ChecklistModel.user_id == current_user.id,
            ChecklistModel.is_active == True
        )
        .first()
    )

    if not checklist:
        raise HTTPException(status_code=404, detail="Checklist not found")

    return checklist