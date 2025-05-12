# File: api/main.py

import os
import json
import threading
import asyncio
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer

# Import workers
from diarization_worker import main as start_diarization_worker
from transcription_worker import main as start_transcription_worker
from prompt_worker import main as start_prompt_worker

# Routers and subscriber registries
from api.routers.auth import router as auth_router
from api.routers.checklists import router as checklist_router
from api.routers.upload import router as upload_router
from api.routers.tasks import (
    router as tasks_router,
    diarization_subscribers,
    transcription_subscribers,
)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("api.main")
# Suppress noisy logs from aiokafka internals
logging.getLogger("aiokafka").setLevel(logging.WARNING)

# Application instance
app = FastAPI(
    title="QA Sales API",
    description="Сервис для диаризации, транскрипции и оценки качества продаж",
    version="0.1.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.getenv("FRONTEND_URL", "*")],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# Register routers
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(checklist_router, prefix="/checklists", tags=["checklists"])
app.include_router(upload_router, prefix="/media", tags=["media"])
app.include_router(tasks_router, prefix="/tasks", tags=["tasks"])

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(',')
DIARIZATION_PROGRESS_TOPIC = os.getenv("KAFKA_DIARIZATION_PROGRESS_TOPIC", "diarization-progress")
TRANSCRIPTION_PROGRESS_TOPIC = os.getenv("KAFKA_TRANSCRIPTION_PROGRESS_TOPIC", "transcription-progress")

@app.on_event("startup")
async def on_startup():
    logger.info("API startup: launching background tasks...")

    # 1) Initialize Kafka consumer before workers to catch events in real time
    app.state.consumer = AIOKafkaConsumer(
        DIARIZATION_PROGRESS_TOPIC,
        TRANSCRIPTION_PROGRESS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="progress-listener",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        loop=asyncio.get_event_loop(),
    )
    await app.state.consumer.start()
    app.state.kafka_task = asyncio.create_task(kafka_listener())
    logger.info("Progress listener task started")

    # 2) Launch background workers
    threading.Thread(
        target=start_diarization_worker,
        name="diarization-worker",
        daemon=True
    ).start()
    logger.info("Diarization worker thread started")

    threading.Thread(
        target=start_transcription_worker,
        name="transcription-worker",
        daemon=True
    ).start()
    logger.info("Transcription worker thread started")

    threading.Thread(
        target=start_prompt_worker,
        name="prompt-worker",
        daemon=True
    ).start()
    logger.info("Prompt worker thread started")

@app.on_event("shutdown")
async def on_shutdown():
    # Stop listener and consumer
    app.state.kafka_task.cancel()
    try:
        await app.state.kafka_task
    except asyncio.CancelledError:
        pass
    await app.state.consumer.stop()
    logger.info("API shutdown: background tasks terminated")

async def kafka_listener():
    """
    Reads messages from Kafka and pushes them to SSE subscribers.
    """
    consumer: AIOKafkaConsumer = app.state.consumer
    try:
        async for msg in consumer:
            data = msg.value
            # logger.info(f"[KafkaListener] topic={msg.topic!r}, partition={msg.partition}, value={data}")
            task_id = data.get("task_id")
            # Select appropriate subscriber list
            if msg.topic == DIARIZATION_PROGRESS_TOPIC:
                queues = diarization_subscribers.get(task_id, set())
            else:
                queues = transcription_subscribers.get(task_id, set())

            for q in list(queues):
                try:
                    q.put_nowait(data)
                except asyncio.QueueFull:
                    pass
    except asyncio.CancelledError:
        pass
