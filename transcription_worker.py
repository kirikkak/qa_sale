# File: transcription_worker.py

import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from tqdm import tqdm  # прогресс-бар в терминале
from openai import OpenAI, OpenAIError
from models import Task as TaskModel

client = OpenAI()

def setup_logger():
    logger = logging.getLogger("transcription_worker")
    # Убираем наследование, чтобы сообщения не дублировались по родительскому логгеру
    logger.propagate = False
    handler = logging.StreamHandler()
    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def main():
    # Загрузка конфигов
    load_dotenv()
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    CONSUMER_TOPIC = os.getenv("TRANSCRIPTION_TOPIC", "transcription")
    PROGRESS_TOPIC = os.getenv("TRANSCRIPTION_PROGRESS_TOPIC", "transcription-progress")
    COMPLETED_TOPIC = os.getenv("TRANSCRIPTION_COMPLETED_TOPIC", "transcription-completed")
    MEDIA_ROOT = os.getenv("MEDIA_ROOT", "media")
    DATABASE_URL = os.getenv("DATABASE_URL")

    # Настройка логгера
    logger = setup_logger()
    # Отключаем логирование HTTPX и OpenAI клиента, чтобы tqdm-бар не прерывался
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logger.info("Starting transcription worker, listening on topic '%s' …", CONSUMER_TOPIC)

    # Настраиваем базу
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # Настраиваем Kafka
    consumer = KafkaConsumer(
        CONSUMER_TOPIC,
        group_id="transcription-worker",
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )

    # Основной цикл получения сообщений
    for msg in consumer:
        try:
            data = msg.value
            # Обрабатываем разные форматы сообщений
            if isinstance(data, dict):
                task_id = data.get("task_id")
            elif isinstance(data, int):
                task_id = data
            elif isinstance(data, str):
                try:
                    task_id = int(data)
                except ValueError:
                    logger.error("Invalid task_id format: %s", data)
                    continue
            else:
                logger.error("Invalid message format: %s", data)
                continue

            logger.info("Received task_id=%s for transcription", task_id)

            # Открываем сессию и проверяем задачу
            session = SessionLocal()
            task = session.get(TaskModel, task_id)
            if not task:
                logger.error("Task %s not found in DB, skipping", task_id)
                session.close()
                continue

            # Папка с сегментами: media/{task_id}
            segments_dir = os.path.join(MEDIA_ROOT, str(task_id))
            if not os.path.isdir(segments_dir):
                logger.error("Segments directory '%s' does not exist", segments_dir)
                session.close()
                continue

            # Собираем все .wav-файлы
            segment_files = sorted([
                f for f in os.listdir(segments_dir)
                if f.lower().endswith(".wav")
            ])
            total = len(segment_files)
            if total == 0:
                logger.error("No .wav segments found in '%s'", segments_dir)
                session.close()
                continue

            # Подготовка
            output_json = os.path.join(segments_dir, "transcript.json")
            results = []
            start_time = time.time()
            progress_bar = tqdm(total=total, desc=f"Transcribing Task {task_id}", unit="seg")

            # Процесс транскрипции сегментов
            for idx, fname in enumerate(segment_files, start=1):
                seg_path = os.path.join(segments_dir, fname)
                text = ""
                try:
                    # Открываем файл в бинарном режиме
                    with open(seg_path, "rb") as audio_file:
                        resp = client.audio.transcriptions.create(
                            file=audio_file,
                            model="whisper-1",
                            temperature=0.0,
                            language = "ru"
                        )
                        text = resp.text
                except OpenAIError as e:
                    logger.exception("Error transcribing segment %s: %s", fname, e)
                except Exception as e:
                    logger.exception("Unexpected error during transcription of %s: %s", fname, e)

                results.append({"segment": fname, "text": text})

                # Подсчёт прогресса и ETA
                elapsed = time.time() - start_time
                percent = int(idx / total * 100)
                eta = int((elapsed / idx) * (total - idx)) if idx else None
                progress_bar.set_postfix({"progress": f"{percent}%", "eta": f"{eta}s"})
                progress_bar.update(1)

                # Публикация прогресса без логирования, чтобы tqdm оставался в одной строке
                progress_msg = {"task_id": task_id, "progress": percent, "eta": eta}
                producer.send(PROGRESS_TOPIC, progress_msg)
                producer.flush()
            progress_bar.close()

            # Сохранение JSON результата с расширенной структурой
            try:
                formatted_results = []

                for segment in results:
                    # Извлекаем имя спикера, номер сегмента и временные метки из имени файла
                    filename_parts = segment["segment"].replace(".wav", "").split("_")

                    # Имя спикера
                    speaker = f"{filename_parts[0]}_{filename_parts[1]}"  # SPEAKER_00

                    # Номер сегмента (удаляем ведущие нули)
                    segment_number = int(filename_parts[2])  # 001 → 1

                    # Начало и конец сегмента
                    start_time = float(".".join(filename_parts[3:5]))
                    end_time = float(".".join(filename_parts[5:7]))

                    # Добавляем сегмент в итоговый результат
                    formatted_results.append({
                        "speaker": speaker,
                        "segment_number": segment_number,
                        "start": start_time,
                        "end": end_time,
                        "text": segment["text"]
                    })

                # Формируем итоговую структуру
                transcript_json = {"transcript": formatted_results}

                # Сохранение файла
                with open(output_json, "w", encoding="utf-8") as fp:
                    json.dump(transcript_json, fp, ensure_ascii=False, indent=2)
                logger.info("Transcript saved to %s", output_json)

            except Exception as e:
                logger.exception("Failed to write transcript JSON: %s", e)

            # Обновление записи в БД
            try:
                task.result_link = os.path.join("media", str(task_id), "transcript.json")
                task.status = "transcribed"
                session.commit()
                logger.info("Task %s updated in DB (result_link=%s)", task_id, task.result_link)
            except Exception as e:
                logger.exception("DB update failed for task %s: %s", task_id, e)
            finally:
                session.close()

            # Публикация завершения
            try:
                producer.send(COMPLETED_TOPIC, {"task_id": task_id})
                producer.flush()
                logger.info("Published completion for task %s", task_id)
            except Exception as e:
                logger.exception("Failed to publish completion for task %s: %s", task_id, e)

        except Exception as e:
            logger.exception("Unhandled error processing message: %s", e)
            continue

if __name__ == "__main__":
    main()
