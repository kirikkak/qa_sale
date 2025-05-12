from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime

# Auth schemas
class RegisterRequest(BaseModel):
    username: str

class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str

class BalanceResponse(BaseModel):
    balance_minutes: int

# Media upload response schema
class MediaUploadResponse(BaseModel):
    task_id: int   # ID созданной задачи
    file_name: str # Уникальное имя сохраненного файла
    duration: int  # Длительность в минутах (округление вверх)

# Checklist schemas
class ChecklistItem(BaseModel):
    step: int
    value: str

class ChecklistCreate(BaseModel):
    name: str
    items: Optional[List[ChecklistItem]]
    sort_order: Optional[int] = 0

class ChecklistUpdate(BaseModel):
    name: Optional[str] = None
    items: Optional[List[ChecklistItem]] = None
    is_active: Optional[bool] = None
    sort_order: Optional[int] = None

class ChecklistRead(BaseModel):
    id: int
    name: str
    items: Optional[List[ChecklistItem]]
    is_active: bool
    sort_order: int
    created_at: datetime

    model_config = {"from_attributes": True}

# Task list response schema
class TaskListResponse(BaseModel):
    task_id: int
    status: str
    checklist_id: Optional[int]
    checklist_name: Optional[str]
    expected_speakers: Optional[int]
    duration: int
    rep_count: int

    model_config = {"from_attributes": True}

class TaskListWithCountResponse(BaseModel):
    tasks: List[TaskListResponse]
    total_count: int

    model_config = {"from_attributes": True}

# Task processing schemas
class ProcessRequest(BaseModel):
    task_id: int

class ProcessResponse(BaseModel):
    task_id: int

class StatusResponse(BaseModel):
    task_id: int
    status: str

class ResultsResponse(BaseModel):
    task_id: int
    result_link: Optional[str] = None

class TaskDetail(BaseModel):
    task_id: int = Field(..., alias='id')   # ← alias!
    status: str
    checklist_id: Optional[int] = None
    expected_speakers: Optional[int] = None
    duration: Optional[int] = None     # в секундах
    media_path: Optional[str] = None

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

# Diarization and transcription schemas
class DiarizationCountResponse(BaseModel):
    rep_count: int

class TranscriptSegment(BaseModel):
    speaker: str
    segment_number: int
    start: float
    end: float
    text: str

# Result schemas

class TranscriptionResponse(BaseModel):
    transcript: List[TranscriptSegment]

class TaskResultItem(BaseModel):
    step: int
    value: str
    result: bool


class TaskResultResponse(BaseModel):
    results: List[TaskResultItem]

# Progress event schema for SSE or WS
class ProgressEvent(BaseModel):
    task_id: int
    progress: int
    eta: Optional[int] = None
    status: Optional[str] = None
    segments: Optional[int] = None

model_config = {"from_attributes": True}