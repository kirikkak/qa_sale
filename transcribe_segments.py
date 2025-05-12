import os
import sys
import warnings
import json
from dotenv import load_dotenv
from openai import OpenAI, OpenAIError
from tqdm import tqdm

# Подавляем избыточные предупреждения
warnings.filterwarnings("ignore", category=UserWarning)

# Загружаем переменные окружения из .env
load_dotenv()

# Инициализируем OpenAI-клиент
client = OpenAI()


def transcribe_segments_to_json(segments_dir: str, output_json: str):
    """
    Транскрибирует все WAV-файлы через Whisper API и сохраняет JSON с полями:
      - segment_id
      - speaker
      - start, end (секунды)
      - text
    Пропускает сегменты с пустой транскрипцией.
    """
    if not os.path.isdir(segments_dir):
        print(f"Папка не найдена: {segments_dir}")
        sys.exit(1)

    entries = []
    wav_files = sorted(f for f in os.listdir(segments_dir) if f.lower().endswith('.wav'))
    if not wav_files:
        print(f"Нет WAV-файлов в папке: {segments_dir}")
        return

    for fname in tqdm(wav_files, desc="Transcribing segments", unit="file"):
        name, _ = os.path.splitext(fname)
        parts = name.split('_')
        if len(parts) < 5:
            continue
        speaker = f"{parts[0]}_{parts[1]}"
        try:
            segment_id = int(parts[2])
            start = float(parts[-2])
            end = float(parts[-1])
        except ValueError:
            continue

        if end - start <= 0.1:
            continue

        path = os.path.join(segments_dir, fname)
        try:
            with open(path, 'rb') as audio_file:
                resp = client.audio.transcriptions.create(
                    model='whisper-1', file=audio_file, language='ru'
                )
            text = resp.text.strip()
        except Exception:
            continue

        if not text:
            continue

        entries.append({
            'segment_id': segment_id,
            'speaker': speaker,
            'start': start,
            'end': end,
            'text': text
        })

    entries.sort(key=lambda x: x['segment_id'])

    try:
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        print(f"JSON сохранён в '{output_json}'")
    except Exception as e:
        print(f"Ошибка при сохранении JSON '{output_json}': {e}")


def main():
    if len(sys.argv) not in (2, 3):
        print("Использование: python transcribe_segments.py path/to/segments_dir [output.json]")
        sys.exit(1)
    segments_dir = sys.argv[1]
    output_json = sys.argv[2] if len(sys.argv) == 3 else "results.json"
    transcribe_segments_to_json(segments_dir, output_json)


if __name__ == '__main__':
    main()
