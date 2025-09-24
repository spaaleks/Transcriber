import re
from datetime import datetime
from pathlib import Path
from typing import Optional
import shutil
import ffmpeg  # requires ffmpeg/ffprobe on PATH

def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s or "job"

def media_duration_seconds(path: Path) -> Optional[float]:
    try:
        # If ffprobe is missing, ffmpeg.probe raises FileNotFoundError
        prob = ffmpeg.probe(str(path))
        dur = prob.get("format", {}).get("duration")
        return float(dur) if dur is not None else None
    except FileNotFoundError:
        # ffprobe not installed or not on PATH
        return None
    except Exception:
        return None

def looks_complete_and_valid(path: Path) -> bool:
    if not path.exists():
        return False
    size = path.stat().st_size
    if size < 128_000:  # 128 KB minimum
        return False
    dur = media_duration_seconds(path)
    # Accept on size when duration cannot be probed; otherwise require dur > 0
    return True if dur is None else (dur > 0)

def hhmmss(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"
