# File: api/routers/tasks.py

import os
import asyncio
import json
import logging
from collections import defaultdict
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload

from api.dependencies import get_db, get_kafka, get_current_user
from models import Task as TaskModel, MinutesTransaction
from api.schemas import (
    ProcessResponse,
    StatusResponse,
    DiarizationCountResponse,
    TranscriptSegment,
    TranscriptionResponse,
    TaskListResponse,
    TaskListWithCountResponse,
    TaskDetail,
    TaskResultResponse,
    TaskResultItem,
)

router = APIRouter()
logger = logging.getLogger("api.tasks")
topic = os.getenv("KAFKA_TASKS_TOPIC", "tasks")
MEDIA_ROOT = os.getenv("MEDIA_ROOT","media")

# --- здесь хранятся подписчики SSE ---
diarization_subscribers   = defaultdict(set)
transcription_subscribers = defaultdict(set)

# Pydantic-модель для запроса обработки задачи
class TaskProcessRequest(BaseModel):
    task_id: int

@router.post(
    "/process",
    response_model=ProcessResponse,
    status_code=status.HTTP_202_ACCEPTED
)
def process_task(
    req: TaskProcessRequest,
    db: Session = Depends(get_db),
    kafka=Depends(get_kafka),
    current_user=Depends(get_current_user)
) -> ProcessResponse:
    task_id = req.task_id

    # Загружаем задачу и проверяем владельца
    task = (
        db.query(TaskModel)
          .filter(TaskModel.id == task_id,
                  TaskModel.user_id == current_user.id)
          .first()
    )
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )

    # Проверяем баланс пользователя
    duration = getattr(task, 'duration', 0)
    if current_user.balance_minutes < duration:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Insufficient minutes: required {duration}, available {current_user.balance_minutes}"
        )

    # Списываем минуты через запись транзакции
    transaction = MinutesTransaction(
        user_id=current_user.id,
        task_id=task.id,
        delta_minutes=-duration,
        comment=f"Debited {duration} minutes for task {task.id}"
    )
    db.add(transaction)
    db.commit()

    # Обновляем статус задачи
    task.status = 'queued_diarization'
    db.commit()

    # Подготавливаем сообщение для Kafka
    message: Dict[str, any] = {
        'task_id': task.id,
        'media_path': task.media_path,
        'checklist': task.checklist.items if task.checklist else []
    }
    # Добавляем ожидаемое число спикеров, если больше нуля
    speakers = getattr(task, 'expected_speakers', 0)
    if speakers:
        message['expected_speakers'] = speakers

    kafka.send(topic, message)
    return ProcessResponse(task_id=task.id)

@router.get(
    "/status/{task_id}",
    response_model=StatusResponse
)
def get_status(
    task_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
) -> StatusResponse:
    task = (
        db.query(TaskModel)
          .filter(TaskModel.id == task_id,
                  TaskModel.user_id == current_user.id)
          .first()
    )
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    return StatusResponse(task_id=task.id, status=task.status)

@router.get(
    "/diarization/{task_id}",
    response_model=DiarizationCountResponse
)
def get_diarization_count(
    task_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
) -> Dict[str, int]:
    task = (
        db.query(TaskModel)
          .filter(TaskModel.id == task_id,
                  TaskModel.user_id == current_user.id)
          .first()
    )
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    rep_count = task.rep_count if task.rep_count is not None else 0
    return {"rep_count": rep_count}

@router.get(
    "/transcription/{task_id}",
    response_model=TranscriptionResponse
)
def get_transcription(
    task_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
) -> TranscriptionResponse:
    # Проверяем, что задача принадлежит текущему пользователю
    task = (
        db.query(TaskModel)
          .filter(TaskModel.id == task_id, TaskModel.user_id == current_user.id)
          .first()
    )
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )

    # Формируем путь к файлу transcript.json
    task_dir = os.path.join(MEDIA_ROOT, str(task_id))
    transcript_file = os.path.join(task_dir, "transcript.json")

    # Проверяем существование файла
    if not os.path.isfile(transcript_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Transcript file not found"
        )

    # Читаем файл transcript.json
    try:
        with open(transcript_file, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Валидируем и форматируем данные
        transcript = [TranscriptSegment(**segment) for segment in raw_data["transcript"]]

    except (OSError, json.JSONDecodeError) as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read transcript file: {str(e)}"
        )

    # Возвращаем содержимое файла
    return TranscriptionResponse(transcript=transcript)


@router.get("/result/{task_id}", response_model=TaskResultResponse)
def get_task_result(task_id: int, db: Session = Depends(get_db), current_user=Depends(get_current_user)) -> TaskResultResponse:
    # Проверяем, что задача принадлежит текущему пользователю
    task = db.query(TaskModel).filter(
        TaskModel.id == task_id,
        TaskModel.user_id == current_user.id
    ).first()
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )

    # Проверяем наличие ссылки на файл результата
    if not task.finished_link:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found"
        )

    # Проверяем существование файла результата
    if not os.path.isfile(task.finished_link):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found"
        )

    # Читаем содержимое файла результата
    try:
        with open(task.finished_link, "r", encoding="utf-8") as f:
            result_data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to read result file: {str(e)}"
        )

    # Возвращаем результат задачи
    return TaskResultResponse(results=[TaskResultItem(**item) for item in result_data])

@router.get("/", response_model=TaskListWithCountResponse)
def get_user_tasks(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100)
) -> TaskListWithCountResponse:
    offset = (page - 1) * page_size

    # Получаем общее количество задач
    total_count = (
        db.query(TaskModel)
        .filter(TaskModel.user_id == current_user.id)
        .count()
    )

    # Получаем задачи с учетом пагинации
    tasks = (
        db.query(TaskModel)
        .filter(TaskModel.user_id == current_user.id)
        .order_by(TaskModel.updated_at.desc())
        .options(joinedload(TaskModel.checklist))
        .offset(offset)
        .limit(page_size)
        .all()
    )

    # Формируем ответ
    response = TaskListWithCountResponse(
        tasks=[
            TaskListResponse(
                task_id=task.id,
                status=task.status,
                checklist_id=task.checklist_id,
                checklist_name=task.checklist.name if task.checklist else None,
                expected_speakers=task.expected_speakers,
                duration=task.duration,
                rep_count=task.rep_count
            )
            for task in tasks
        ],
        total_count=total_count
    )

    return response

@router.get("/{task_id}", response_model=TaskDetail)
def task_detail(task_id: int, db: Session = Depends(get_db)):
    task = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if not task:
        raise HTTPException(404, "Task not found")
    return task

# SSE для диаризации
@router.get("/diarization/stream/{task_id}")
async def diarization_stream(request: Request, task_id: int):
    q = asyncio.Queue(maxsize=10)
    diarization_subscribers[task_id].add(q)
    logger.info("Client subscribed to diarization progress for task %s", task_id)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                data = await q.get()
                yield f"data: {json.dumps(data)}\n\n"
        finally:
            diarization_subscribers[task_id].discard(q)
            logger.info("Client unsubscribed from diarization progress for task %s", task_id)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# SSE для транскрипции
@router.get("/transcription/stream/{task_id}")
async def transcription_stream(request: Request, task_id: int):
    q = asyncio.Queue(maxsize=10)
    transcription_subscribers[task_id].add(q)
    logger.info("Client subscribed to transcription progress for task %s", task_id)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                data = await q.get()
                yield f"data: {json.dumps(data)}\n\n"
        finally:
            transcription_subscribers[task_id].discard(q)
            logger.info("Client unsubscribed from transcription progress for task %s", task_id)

    return StreamingResponse(event_generator(), media_type="text/event-stream")