from pathlib import Path
from typing import Optional, Callable
from faster_whisper import WhisperModel
from .utils import media_duration_seconds, hhmmss

def transcribe_to_txt(media_path: Path, txt_path: Path,
                      model_size: str, device: str, compute_type: str, cpu_threads: int,
                      progress_cb: Callable[[float], None], log_cb: Callable[[str], None],
                      lang_hint: Optional[str] = None):
    model = WhisperModel(model_size, device=device, compute_type=compute_type, cpu_threads=cpu_threads)
    segments, info = model.transcribe(
        str(media_path),
        language=lang_hint,
        task="transcribe",
        beam_size=1,
        vad_filter=False,
        condition_on_previous_text=False,
        word_timestamps=False,
    )
    dur = media_duration_seconds(media_path) or getattr(info, "duration", None) or None

    with open(txt_path, "w", encoding="utf-8") as txt_f:
        if getattr(info, "language", None) is not None:
            log_cb(f"Detected language: {info.language} (p={getattr(info,'language_probability',None)})")
        seen_end = 0.0
        for seg in segments:
            line = seg.text.strip()
            txt_f.write(line + "\n")
            if dur and seg.end:
                pct = min(100.0, float(seg.end) / float(dur) * 100.0)
                progress_cb(pct / 100.0)
                if seg.end - seen_end >= 2.0:
                    log_cb(f"{pct:5.1f}% [{hhmmss(seg.start)} → {hhmmss(seg.end)}] {line}")
                    seen_end = seg.end
    progress_cb(1.0)
    log_cb("Transcription completed. Writing finalized outputs.")
