import os
import json
import uuid
import logging
import warnings
import torch
import time
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pyannote.audio import Pipeline
from pyannote.audio.pipelines.utils.hook import ProgressHook
from pydub import AudioSegment
from dotenv import load_dotenv
from models import Task as TaskModel

# Подавляем избыточные warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", module="torchaudio")

# Загружаем переменные окружения
load_dotenv()

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
TASKS_TOPIC         = os.getenv("KAFKA_TASKS_TOPIC", "tasks")
PROGRESS_TOPIC      = os.getenv("KAFKA_PROGRESS_TOPIC", "diarization-progress")
TRANSCRIPTION_TOPIC = os.getenv("KAFKA_TRANSCRIPTION_TOPIC", "transcription")
DATABASE_URL        = os.getenv("DATABASE_URL")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("diarization_worker")
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

# Инициализация БД
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class ProgressPublisher(ProgressHook):
    """
    Hook для публикации прогресса + ETA в Kafka — только для этапа embeddings.
    """
    def __init__(self, producer, task_id):
        super().__init__()
        self.producer = producer
        self.task_id  = task_id
        self._start   = None

    def __call__(self, *args, **kwargs):
        # Определяем имя этапа
        step_name = args[0] if args else kwargs.get("step_name") or kwargs.get("step")
        # Если не embeddings — просто обновляем консоль и выходим
        if step_name not in ("embedding", "embeddings"):
            return super().__call__(*args, **kwargs)

        # Инициализируем время старта при первом вызове
        if self._start is None:
            self._start = time.time()

        completed = kwargs.get("completed", 0) or 0
        total     = kwargs.get("total", 1) or 1

        # Процент
        percent = int(completed / total * 100) if total else 0

        # Вычисляем ETA (в секундах)
        elapsed = time.time() - self._start
        eta = int((elapsed / completed) * (total - completed)) if completed else None

        # Формируем и шлём сообщение
        msg = {
            "task_id": self.task_id,
            "progress": percent,
            # если completed==0, ETA может быть None
            **({"eta": eta} if eta is not None else {})
        }
        try:
            self.producer.send(PROGRESS_TOPIC, msg)
        except Exception:
            logger.exception("Failed to send embeddings progress for task %s", self.task_id)

        # Обновляем консольный вывод как обычно
        return super().__call__(*args, **kwargs)

def prepare_audio(input_path: str) -> str:
    """Конвертирует аудио/видео в WAV 16kHz моно."""
    ext = os.path.splitext(input_path)[1].lower().lstrip('.')
    audio = AudioSegment.from_file(input_path, format=ext)
    audio = audio.set_frame_rate(16000).set_channels(1)
    out_path = f"temp_{os.getpid()}_{uuid.uuid4().hex}.wav"
    audio.export(out_path, format="wav")
    logger.info("Prepared audio '%s' -> '%s'", input_path, out_path)
    return out_path


def main():
    logger.info("=== Diarization worker startup ===")

    # Kafka Consumer
    try:
        consumer = KafkaConsumer(
            TASKS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="diarization-worker",
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )
        logger.info("Consumer subscribed to '%s'", TASKS_TOPIC)
    except Exception:
        logger.exception("Failed to initialize Kafka consumer")
        return

    # Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info("Producer ready")
    except Exception:
        logger.exception("Failed to initialize Kafka producer")
        return

    # Загрузка пайплайна диаризации
    try:
        device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        pipeline = Pipeline.from_pretrained(
            "pyannote/speaker-diarization-3.1",
            use_auth_token=os.getenv("HUGGINGFACE_TOKEN")
        ).to(device)
        logger.info("Diarization pipeline loaded on %s", device)
    except Exception:
        logger.exception("Failed to load diarization pipeline")
        return

    logger.info("Worker is listening for tasks...")

    for message in consumer:
        data       = message.value
        task_id    = data.get("task_id")
        media_path = data.get("media_path")
        expected   = data.get("expected_speakers")
        logger.info("Task %s received: %s", task_id, data)

        try:
            # Подготовка аудио
            wav_file = prepare_audio(media_path)

            # Настройка ProgressPublisher (hook)
            hook = ProgressPublisher(producer, task_id)
            call_args = {}
            if expected is not None:
                call_args["min_speakers"] = expected
                call_args["max_speakers"] = expected

            # Запуск пайплайна с hook
            logger.info("Starting diarization for task %s", task_id)
            with hook as active_hook:
                diarization = pipeline(wav_file, hook=active_hook, **call_args)

            # Собираем сегменты
            audio = AudioSegment.from_file(wav_file)
            groups = []
            for turn, _, spk in diarization.itertracks(yield_label=True):
                if groups and groups[-1]["speaker"] == spk and (turn.start - groups[-1]["end"]) <= 0.2:
                    groups[-1]["end"] = turn.end
                else:
                    groups.append({"speaker": spk, "start": turn.start, "end": turn.end})

            # (Опционально) отправляем финальный 100% только если хотите
            try:
                producer.send(PROGRESS_TOPIC, {"task_id": task_id, "progress": 100})
                producer.flush()
            except Exception:
                logger.exception("Failed to send final 100% for task %s", task_id)

            # Группировка сегментов
            groups = []
            for turn, _, spk in diarization.itertracks(yield_label=True):
                duration = turn.end - turn.start
                if groups and groups[-1]["speaker"] == spk and (turn.start - groups[-1]["end"]) <= 0.2:
                    groups[-1]["end"] = turn.end
                else:
                    groups.append({"speaker": spk, "start": turn.start, "end": turn.end})

            # Экспорт сегментов с расширенными именами файлов
            out_dir = os.path.join(os.path.dirname(media_path), str(task_id))
            os.makedirs(out_dir, exist_ok=True)

            for idx, grp in enumerate(groups, start=1):
                start_time = f"{grp['start']:.1f}".replace(".", "_")
                end_time = f"{grp['end']:.1f}".replace(".", "_")
                segment_filename = f"{grp['speaker']}_{idx:03d}_{start_time}_{end_time}.wav"
                segment_path = os.path.join(out_dir, segment_filename)

                # Экспортируем сегмент
                seg = audio[int(grp["start"] * 1000):int(grp["end"] * 1000)]
                seg.export(segment_path, format="wav")

            # Вычисляем число уникальных спикеров
            unique_speakers = {grp["speaker"] for grp in groups}
            num_speakers = len(unique_speakers)

            # Удаление временных файлов
            try:
                os.remove(wav_file)
                logger.info("Removed temporary file '%s'", wav_file)
            except OSError as e:
                logger.warning("Failed to remove temporary file '%s': %s", wav_file, e)

            try:
                os.remove(media_path)
                logger.info("Removed original file '%s'", media_path)
            except OSError as e:
                logger.warning("Failed to remove original file '%s': %s", media_path, e)

            # Обновление статуса в БД
            db = SessionLocal()
            task = db.get(TaskModel, task_id)
            if task:
                task.status    = "diarized"
                task.rep_count = len(groups)
                if expected is None:
                    task.expected_speakers = num_speakers
                else:
                    task.expected_speakers = expected
                db.commit()
            db.close()
            logger.info("Task %s updated in DB", task_id)

            # Отправка на транскрипцию
            producer.send(TRANSCRIPTION_TOPIC, {"task_id": task_id})
            producer.flush()
            logger.info("Transcription enqueued for task %s", task_id)

        except Exception as exc:
            logger.exception("Failed to process task %s: %s", task_id, exc)


if __name__ == "__main__":
    main()
