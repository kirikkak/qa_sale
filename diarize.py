import os
import sys
import warnings
import subprocess
import json
from dotenv import load_dotenv
from pyannote.audio import Pipeline
from pyannote.audio.pipelines.utils.hook import ProgressHook
from pydub import AudioSegment
from tqdm import tqdm
import torch

# Подавляем избыточные предупреждения
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", module="torchaudio")

# Загружаем переменные окружения из .env
load_dotenv()


def check_audio_tracks(input_path: str) -> int:
    """
    Определяет число аудио-потоков в файле с помощью ffprobe.
    """
    try:
        cmd = [
            'ffprobe', '-v', 'error', '-select_streams', 'a',
            '-show_entries', 'stream=index',
            '-of', 'json', input_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        info = json.loads(result.stdout)
        return len(info.get('streams', []))
    except Exception:
        return 0


def prepare_audio(input_path: str) -> str:
    """
    Конвертирует входной файл (mp3, wav, webm, mp4 и т.д.) в моно WAV 16 kHz.
    """
    ext = os.path.splitext(input_path)[1][1:].lower()
    audio = AudioSegment.from_file(input_path, format=ext)
    audio = audio.set_frame_rate(16000).set_channels(1)
    out_path = "temp_16k_mono.wav"
    audio.export(out_path, format="wav")
    return out_path


def diarize_file(audio_path: str, expected_speakers: int = None):
    # Подготовка аудио
    wav_path = prepare_audio(audio_path)

    # Определение устройства
    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"Используем устройство: {device}")

    # Загрузка модели диаризации
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token=os.getenv("HUGGINGFACE_TOKEN")
    )
    pipeline.to(device)

        # Настройка количества спикеров
    call_args = {"hook": ProgressHook()}
    if expected_speakers is not None:
        # Используем ключи min_speakers и max_speakers для Pipeline
        call_args.update({"min_speakers": expected_speakers, "max_speakers": expected_speakers})

    # Диаризация с прогресс-баром и опциональным числом спикеров с прогресс-баром и опциональным числом спикеров
    with call_args.pop("hook") as hook:
        diarization = pipeline(wav_path, hook=hook, **call_args)

    # Загрузка для нарезки
    audio = AudioSegment.from_file(wav_path)

    # Группировка сегментов и прикрепление коротких до 0.2 сек
    groups = []
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        duration = turn.end - turn.start
        if duration <= 0.2 and groups:
            groups[-1]["end"] = turn.end
            continue
        if groups and groups[-1]["speaker"] == speaker:
            groups[-1]["end"] = turn.end
        else:
            groups.append({"speaker": speaker, "start": turn.start, "end": turn.end})

    # Экспорт сегментов
    base = os.path.splitext(os.path.basename(audio_path))[0]
    output_dir = f"{base}_segments"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Экспортируем {len(groups)} сегментов в '{output_dir}'...")

    for i, group in enumerate(tqdm(groups, desc="Exporting segments", unit="seg"), start=1):
        start_ms = int(group["start"] * 1000)
        end_ms = int(group["end"] * 1000)
        segment = audio[start_ms:end_ms]
        fname = f"{group['speaker']}_{i:03d}_{group['start']:.1f}_{group['end']:.1f}.wav"
        segment.export(os.path.join(output_dir, fname), format="wav")

    print(f"Готово, сегменты сохранены в '{output_dir}'")

    # Удаление временного файла
    try:
        os.remove(wav_path)
    except OSError as e:
        print(f"Не удалось удалить временный файл: {e}")


if __name__ == '__main__':
    # Парсинг аргументов
    if len(sys.argv) not in (2, 3):
        print("Использование: python diarize.py path/to/audio [expected_speakers]")
        sys.exit(1)
    audio_path = sys.argv[1]
    expected = int(sys.argv[2]) if len(sys.argv) == 3 else None
    diarize_file(audio_path, expected)
