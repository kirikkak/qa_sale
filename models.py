from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, JSON, Boolean, event
)
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    hashed_password = Column(String(128), nullable=False)
    role = Column(String(20), nullable=False, default='user')  # 'admin' or 'user'
    is_active = Column(Boolean, nullable=False, default=True)
    balance_minutes = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    checklists = relationship(
        'Checklist', back_populates='user', cascade='all, delete-orphan'
    )
    tasks = relationship(
        'Task', back_populates='user', cascade='all, delete-orphan'
    )
    transactions = relationship(
        'MinutesTransaction', back_populates='user', cascade='all, delete-orphan'
    )


class Checklist(Base):
    __tablename__ = 'checklists'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(100), nullable=False)
    items = Column(JSON, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship('User', back_populates='checklists')
    tasks = relationship('Task', back_populates='checklist', cascade='all, delete-orphan')


class Task(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    checklist_id = Column(Integer, ForeignKey('checklists.id', ondelete='SET NULL'))
    media_path = Column(String, nullable=False)  # Path to uploaded media (audio/video)
    status = Column(String(20), nullable=False, default='uploaded')
    result_link = Column(String, nullable=True)
    finished_link = Column(String, nullable=True)
    expected_speakers = Column(Integer, nullable=True)  # количество ожидаемых спикеров
    duration = Column(Integer, nullable=False, default=0) # длительность файла в минутах
    rep_count = Column(Integer, nullable=False, default=0) # количество реплик
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )

    # Relationships
    user = relationship('User', back_populates='tasks')
    checklist = relationship('Checklist', back_populates='tasks')


class MinutesTransaction(Base):
    __tablename__ = 'minutes_transactions'

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    task_id = Column(Integer, ForeignKey('tasks.id', ondelete='SET NULL'), nullable=True)
    delta_minutes = Column(Integer, nullable=False)  # positive for credit, negative for debit
    comment = Column(String(200), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship('User', back_populates='transactions')
    task = relationship('Task')


