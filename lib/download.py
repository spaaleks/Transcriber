from pathlib import Path
from typing import Optional, Callable, Tuple
import yt_dlp
from .utils import looks_complete_and_valid, media_duration_seconds

class ResumeRejected(Exception):
    pass

def ydl_download_resumable(url: str, base_out_stem: Path,
                           progress_cb: Callable[[float], None],
                           allow_resume: bool,
                           use_external: bool = False) -> Tuple[Path, Optional[float]]:
    def hook(d):
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            if total > 0:
                progress_cb(min(1.0, done / total))
        elif d.get("status") == "finished":
            progress_cb(1.0)

    ydl_opts = {
        "outtmpl": str(base_out_stem),
        "retries": 10,
        "fragment_retries": 10,
        "merge_output_format": "mp4",
        "concurrent_fragment_downloads": 4,
        "continuedl": bool(allow_resume),
        "nopart": False,
        "quiet": True,
        "progress_hooks": [hook],
        "http_chunk_size": 1 << 20,      # 1 MiB
    }
    if use_external:
        # If aria2c is installed, yt-dlp will use it. Otherwise ffmpeg. Both are more robust for some servers.
        ydl_opts["external_downloader"] = "aria2c,ffmpeg"

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            fn = Path(ydl.prepare_filename(info))
            if not fn.exists():
                cand = None
                if "requested_downloads" in info:
                    cand = info["requested_downloads"][0].get("filepath")
                if cand:
                    fn = Path(cand)
            if not fn.exists():
                # best-effort scan
                for p in base_out_stem.parent.glob(base_out_stem.name + ".*"):
                    if p.is_file():
                        fn = p
                        break
            if not fn.exists():
                raise RuntimeError("Download finished but output file not found.")
            # expected duration from metadata, if known
            exp = None
            try:
                d = info.get("duration")
                if d is not None:
                    exp = float(d)
            except Exception:
                exp = None
            return fn, exp
    except yt_dlp.utils.DownloadError as e:
        msg = str(e).lower()
        if "http error 416" in msg or "requested range not satisfiable" in msg:
            raise ResumeRejected from e
        raise

def download_with_resume_and_validation(url: str, base_out_stem: Path,
                                        progress_cb: Callable[[float], None],
                                        log_cb: Callable[[str], None]) -> Path:
    # 1. Try resume if a .part exists
    try:
        log_cb("Starting download (resume if possible)")
        final_media, exp = ydl_download_resumable(url, base_out_stem, progress_cb, allow_resume=True)
    except ResumeRejected:
        log_cb("Server rejected resume (416). Retrying fresh.")
        final_media, exp = ydl_download_resumable(url, base_out_stem, progress_cb, allow_resume=False)

    # 2. Validate size/duration and guard against truncated partial content
    def _valid(p: Path, exp_dur: Optional[float]) -> bool:
        if not looks_complete_and_valid(p):
            return False
        if exp_dur is None:
            return True
        got = media_duration_seconds(p)
        # accept if within 90% of expected duration
        return (got is not None) and (got >= 0.90 * exp_dur)

    if not _valid(final_media, exp):
        size1 = final_media.stat().st_size if final_media.exists() else 0
        dur1 = media_duration_seconds(final_media)
        log_cb(f"Validation failed after download: size={size1} dur={dur1} expected={exp}")
        try:
            final_media.unlink(missing_ok=True)
        except Exception:
            pass
        # 3. Final attempt: no-resume + external downloader for flaky partial servers
        log_cb("Retrying with no-resume and external downloader.")
        final_media, exp2 = ydl_download_resumable(
            url, base_out_stem, progress_cb, allow_resume=False, use_external=True
        )
        if not _valid(final_media, exp2 or exp):
            size2 = final_media.stat().st_size if final_media.exists() else 0
            dur2 = media_duration_seconds(final_media)
            raise RuntimeError(f"Media corrupted after retry. size={size2} dur={dur2} expected={exp2 or exp}")

    return final_media
