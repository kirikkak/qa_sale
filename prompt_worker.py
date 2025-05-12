import os
import json
import logging
from openai import OpenAI
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from models import Task, Checklist
from datetime import datetime


def setup_logger():
    """Настройка логирования"""
    logger = logging.getLogger("prompt_worker")
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


def generate_prompt(transcript, checklist_items):
    """Генерация промпта для OpenAI на основе транскрипта и чеклиста"""
    prompt_template = '''Транскрипт: "{transcript}"
Проверить соответствие шагам:
{steps}
Вернуть результат в формате:
[{{"step": 1, "result": true}}, {{"step": 2, "result": false}}, ...] для всех шагов с первого до последнего'''

    steps_text = "\n".join([f"{index + 1}. {item}" for index, item in enumerate(checklist_items)])
    return prompt_template.format(transcript=transcript, steps=steps_text)


def process_task(task_id, session, openai_client, openai_model, logger):
    """Обработка задачи с указанным ID"""
    try:
        # Получаем задачу из базы данных
        task_model = session.query(Task).filter_by(id=task_id).first()
        if not task_model or not task_model.checklist_id:
            logger.error(f"Задача с ID {task_id} не найдена или не содержит чеклист")
            return False

        # Получаем чеклист
        checklist = session.query(Checklist).filter_by(id=task_model.checklist_id).first()
        if not checklist or not checklist.items:
            logger.error(f"Чеклист с ID {task_model.checklist_id} не найден или пуст")
            return False

        # Проверяем существование файла транскрипции
        transcript_file = task_model.result_link
        if not os.path.exists(transcript_file):
            logger.error(f"Файл транскрипции не найден: {transcript_file}")
            return False

        logger.info(f"Обработка транскрипции: {transcript_file}")

        # Читаем файл транскрипции
        try:
            with open(transcript_file, "r", encoding="utf-8") as f:
                transcript_data = json.load(f)
        except json.JSONDecodeError:
            logger.error(f"Ошибка чтения JSON из файла {transcript_file}")
            return False

        # Формируем текст транскрипции
        if isinstance(transcript_data, dict) and "transcript" in transcript_data:
            transcript_text = " ".join([segment.get("text", "") for segment in transcript_data.get("transcript", [])])
        else:
            logger.error(f"Неверный формат данных транскрипции в файле {transcript_file}")
            return False

        # Формируем промпт
        prompt = generate_prompt(transcript_text, checklist.items)
        logger.info(f"Сформирован промпт для задачи {task_id}")

        # Запрос к OpenAI API
        try:
            response = openai_client.chat.completions.create(
                model=openai_model,
                messages=[
                    {"role": "system",
                     "content": "Проверь выполнение шагов в транскрипции. Верни результат в формате JSON массива с полями step и result (true/false) для каждого шага с первого до последнего."},
                    {"role": "user", "content": prompt}
                ]
            )

            # Извлекаем JSON из ответа
            response_content = response.choices[0].message.content.strip()
            results = json.loads(response_content)
            logger.info(f"Получен результат от OpenAI для задачи {task_id}")

        except Exception as e:
            logger.error(f"Ошибка при запросе к OpenAI API: {e}")
            return False

        # Обновляем результаты в чеклисте
        checklist_items = checklist.items.copy()
        for result in results:
            step_index = result.get("step", 0) - 1
            if 0 <= step_index < len(checklist_items):
                checklist_items[step_index]["result"] = result.get("result", False)

        # Сохранение результата рядом с файлом транскрипции
        result_file = os.path.splitext(transcript_file)[0] + "_results.json"
        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(checklist_items, f, ensure_ascii=False, indent=4)

        # Обновляем статус задачи
        task_model.status = "finished"
        task_model.finished_link = result_file
        task_model.updated_at = datetime.utcnow()
        session.commit()

        logger.info(f"Результаты сохранены в {result_file} и задача {task_id} обновлена")
        return True

    except Exception as e:
        logger.error(f"Ошибка обработки задачи {task_id}: {e}")
        session.rollback()
        return False


def main():
    """Основная функция воркера"""
    # Настройка логирования
    logger = setup_logger()
    logger.info("Запуск воркера для обработки промптов")

    # Загрузка конфигов из .env файла
    load_dotenv()

    # Получение настроек из переменных окружения
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_input = os.getenv("KAFKA_TOPIC_INPUT", "transcription-completed")
    database_url = os.getenv("DATABASE_URL")

    # Проверка обязательных настроек
    if not openai_api_key:
        logger.error("OPENAI_API_KEY не указан в переменных окружения")
        return

    if not database_url:
        logger.error("DATABASE_URL не указан в переменных окружения")
        return

    logger.info(f"Используется модель OpenAI: {openai_model}")
    logger.info(f"Подключение к Kafka: {kafka_bootstrap_servers}, топик: {kafka_topic_input}")

    # Инициализация OpenAI клиента
    openai_client = OpenAI(api_key=openai_api_key)

    # Подключение к базе данных
    try:
        engine = create_engine(database_url)
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        session = Session()
        logger.info("Подключение к базе данных успешно установлено")
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return

    # Инициализация Kafka-консьюмера
    try:
        consumer = KafkaConsumer(
            kafka_topic_input,
            group_id="prompt-worker",
            bootstrap_servers=kafka_bootstrap_servers.split(","),
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        logger.info(f"Kafka-консьюмер инициализирован, слушаем топик '{kafka_topic_input}'")
    except Exception as e:
        logger.error(f"Ошибка инициализации Kafka-консьюмера: {e}")
        session.close()
        return

    # Основной цикл обработки сообщений
    try:
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for tp, msgs in messages.items():
                for message in msgs:
                    task = message.value
                    task_id = task.get("task_id")

                    if not task_id:
                        logger.warning("Получено сообщение без task_id, пропускаем")
                        continue

                    logger.info(f"Получено сообщение из Kafka с task_id: {task_id}")
                    process_task(task_id, session, openai_client, openai_model, logger)

    except KeyboardInterrupt:
        logger.info("Работа воркера прервана пользователем. Завершаем...")
    except Exception as e:
        logger.error(f"Неожиданная ошибка в основном цикле: {e}")
    finally:
        consumer.close()
        session.close()
        logger.info("Соединения закрыты. Воркер завершен.")


if __name__ == "__main__":
    main()