import os
import uuid
import math
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydub import AudioSegment

from api.dependencies import get_db, get_current_user
from models import Task as TaskModel
from api.schemas import MediaUploadResponse

router = APIRouter()

# Настройки: допустимые расширения и максимальный размер файла (1 ГБ по умолчанию)
ALLOWED_EXTENSIONS = set(
    os.getenv("ALLOWED_EXTENSIONS", ".wav,.mp3,.webm,.mp4").split(",")
)
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 1 * 1024 * 1024 * 1024))  # байты

# Директория для медиа-файлов
MEDIA_DIR = os.getenv("MEDIA_DIR", "media")

os.makedirs(MEDIA_DIR, exist_ok=True)

@router.post(
    "/upload",
    response_model=MediaUploadResponse,
    status_code=status.HTTP_201_CREATED
)
def upload_media(
    file: UploadFile = File(...),
    checklist_id: int = Form(...),
    expected_speakers: int | None = Form(None),
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    """
    Загружает аудио/видео, сохраняет файл, проверяет формат и размер,
    вычисляет длительность (минуты, округление вверх),
    сохраняет задачу с переданным checklist_id и expected_speakers,
    возвращает уникальное имя файла и длительность.
    """
    # Проверка расширения файла
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail=f"Unsupported file type: {ext}"
        )

    # Генерация уникального имени и путь сохранения
    unique_name = f"{uuid.uuid4().hex}{ext}"
    file_path = os.path.join(MEDIA_DIR, unique_name)

    # Сохранение файла по частям с проверкой размера
    total_size = 0
    try:
        with open(file_path, "wb") as buffer:
            while chunk := file.file.read(1024 * 1024):  # читаем по 1 МБ
                total_size += len(chunk)
                if total_size > MAX_FILE_SIZE:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail="File size exceeds limit"
                    )
                buffer.write(chunk)
    except HTTPException:
        # Удаляем частично записанный файл
        try:
            os.remove(file_path)
        except OSError:
            pass
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file: {e}"
        )

    # Вычисление длительности в минутах (округление вверх)
    try:
        audio = AudioSegment.from_file(file_path)
        duration_ms = len(audio)
        duration_min = math.ceil(duration_ms / 1000 / 60)
    except Exception:
        duration_min = 0

    # Создание записи задачи в БД
    task = TaskModel(
        user_id=current_user.id,
        media_path=file_path,
        status='uploaded',
        checklist_id=checklist_id,
        expected_speakers=expected_speakers,
        duration=duration_min
    )
    db.add(task)
    db.commit()
    db.refresh(task)

    return MediaUploadResponse(
        task_id=task.id,
        file_name=unique_name,
        duration=duration_min
    )
