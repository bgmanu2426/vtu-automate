from __future__ import annotations

import asyncio
import hmac
import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable
from urllib.parse import quote, unquote, urlparse

import httpx
from fastapi import FastAPI, HTTPException
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field


def _load_local_env_files() -> None:
    """Load .env then .env.local if present, without overriding real OS env vars."""
    root = Path(__file__).resolve().parent
    merged: dict[str, str] = {}

    for name in (".env", ".env.local"):
        path = root / name
        if not path.exists():
            continue

        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                continue

            if (value.startswith('"') and value.endswith('"')) or (
                value.startswith("'") and value.endswith("'")
            ):
                value = value[1:-1]

            merged[key] = value

    for key, value in merged.items():
        if key not in os.environ:
            os.environ[key] = value


_load_local_env_files()


VTU_API_BASE_URL = os.getenv("VTU_API_BASE_URL", "").rstrip("/")
GITHUB_URL = os.getenv("GITHUB_URL", "https://github.com/vikas-bhat-d/vtu-course-automation")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
CORS_ORIGIN = os.getenv("CORS_ORIGIN", "*")
KV_REST_API_URL = os.getenv("KV_REST_API_URL", "").rstrip("/")
KV_REST_API_TOKEN = os.getenv("KV_REST_API_TOKEN", "")
STUDENTS_SEED = 40
QUEUE_KEY = "autopilot:queue"
JOB_PREFIX = "autopilot:job:"
DEDUP_PREFIX = "autopilot:dedup:"


def env_int(*keys: str, default: int) -> int:
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        try:
            return int(raw)
        except ValueError:
            continue
    return default


@dataclass
class RuntimeConfig:
    max_concurrent: int = env_int("MAX_CONCURRENT", default=2)
    batch_size: int = env_int("DEFAULT_BATCH_SIZE", "VTU_BATCH_SIZE", default=10)
    max_attempts: int = env_int("DEFAULT_MAX_ATTEMPTS", "VTU_MAX_ATTEMPTS", default=50)
    retry_delay_ms: int = int(os.getenv("RETRY_DELAY_MS", "2000"))
    request_delay_ms: int = int(os.getenv("REQUEST_DELAY_MS", "500"))


@dataclass
class JobConfig:
    email: str
    password: str
    course_slug: str
    batch_size: int
    max_attempts: int


@dataclass
class JobState:
    id: str
    status: str
    position: int
    dedup_key: str
    logs: list[dict[str, Any]] = field(default_factory=list)
    total: int = 0
    processed: int = 0
    progress: int = 0
    created_at: int = field(
        default_factory=lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    )
    result: dict[str, Any] | None = None


class SubmitPayload(BaseModel):
    email: str = Field(min_length=3, max_length=100)
    password: str = Field(min_length=1, max_length=128)
    courseSlug: str = Field(min_length=1, max_length=500)
    batchSize: int | None = Field(default=None, ge=1, le=50)
    maxAttempts: int | None = Field(default=None, ge=1, le=500)


app = FastAPI(title="VTU Autopilot FastAPI")
app.mount("/frontend", StaticFiles(directory="frontend", html=True), name="frontend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[CORS_ORIGIN] if CORS_ORIGIN != "*" else ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH", "OPTIONS"],
    allow_headers=["*"],
)

runtime_config = RuntimeConfig()

jobs: dict[str, JobState] = {}
queue: list[tuple[str, JobConfig]] = []
sse_connections: dict[str, set[asyncio.Queue[tuple[str, dict[str, Any]]]]] = {}
active_job_keys: dict[str, str] = {}
notification_state: dict[str, Any] = {"message": "", "disabled": False}

active_jobs = 0
state_lock = asyncio.Lock()
mem_students = STUDENTS_SEED
mem_lectures = 0
submit_rate: dict[str, list[int]] = {}
submit_rate_lock = asyncio.Lock()
SUBMIT_WINDOW_MS = 15 * 60 * 1000
SUBMIT_MAX = 5

CONFIG_BOUNDS: dict[str, tuple[int, int]] = {
    "maxConcurrent": (1, 10),
    "batchSize": (1, 50),
    "maxAttempts": (1, 500),
    "retryDelay": (0, 30000),
    "requestDelay": (0, 10000),
}


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def parse_duration(raw: str | None) -> int:
    if not raw:
        return 0
    parts = [int(p) for p in raw.replace(" mins", "").strip().split(":") if p.strip().isdigit()]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    return 0


def is_uuid_v4(job_id: str) -> bool:
    return bool(
        re.match(
            r"^[\da-f]{8}-[\da-f]{4}-4[\da-f]{3}-[89ab][\da-f]{3}-[\da-f]{12}$",
            job_id,
            re.IGNORECASE,
        )
    )


def sse_chunk(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def push(job_id: str, event: str, data: dict[str, Any]) -> None:
    async with state_lock:
        targets = list(sse_connections.get(job_id, set()))
    for conn in targets:
        try:
            conn.put_nowait((event, data))
        except asyncio.QueueFull:
            continue


def sanitize_slug(slug: str) -> bool:
    return bool(re.match(r"^[\w-]+$", slug))


def extract_course_slug(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""

    if value.startswith("http://") or value.startswith("https://"):
        parsed = urlparse(value)
        segments = [seg for seg in parsed.path.strip("/").split("/") if seg]
        if not segments:
            return ""

        if "learning" in segments:
            idx = segments.index("learning")
            if idx + 1 < len(segments):
                return unquote(segments[idx + 1]).strip()

        return unquote(segments[-1]).strip()

    return value


def snapshot(job: JobState) -> dict[str, Any]:
    return {
        "id": job.id,
        "status": job.status,
        "progress": job.progress,
        "total": job.total,
        "processed": job.processed,
        "position": job.position,
        "logs": job.logs[-50:],
        "result": job.result,
    }


def api_base_candidates(base_url: str) -> list[str]:
    base = base_url.rstrip("/")
    if not base:
        return []

    candidates = [base]
    if base.endswith("/api/v1"):
        candidates.append(base[: -len("/v1")])
    elif base.endswith("/api"):
        candidates.append(f"{base}/v1")

    # Keep order stable while de-duplicating.
    ordered: list[str] = []
    for item in candidates:
        if item and item not in ordered:
            ordered.append(item)
    return ordered


def _upstash_enabled() -> bool:
    return bool(KV_REST_API_URL and KV_REST_API_TOKEN)


async def _upstash_request(path: str, payload: list[Any]) -> Any | None:
    if not _upstash_enabled():
        return None

    url = f"{KV_REST_API_URL}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {KV_REST_API_TOKEN}"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return (resp.json() or {}).get("result")


async def _redis_cmd(cmd: str, *args: Any) -> Any | None:
    encoded = "/".join(quote(str(arg), safe="") for arg in args)
    path = f"{cmd}/{encoded}" if encoded else cmd
    return await _upstash_request(path, [])


async def ensure_seed() -> None:
    if not _upstash_enabled():
        return
    try:
        await _upstash_request("set/autopilot:students/40", ["NX"])
    except Exception:
        return


async def get_stats() -> dict[str, Any]:
    if not _upstash_enabled():
        return {
            "studentsHelped": mem_students,
            "lecturesCompleted": mem_lectures,
            "githubUrl": GITHUB_URL,
        }

    try:
        students_raw, lectures_raw = await asyncio.gather(
            _upstash_request("get/autopilot:students", []),
            _upstash_request("get/autopilot:lectures", []),
        )
        students = max(int(students_raw or STUDENTS_SEED), STUDENTS_SEED)
        lectures = int(lectures_raw or 0)
        return {
            "studentsHelped": students,
            "lecturesCompleted": lectures,
            "githubUrl": GITHUB_URL,
        }
    except Exception:
        return {
            "studentsHelped": mem_students,
            "lecturesCompleted": mem_lectures,
            "githubUrl": GITHUB_URL,
        }


async def record_job_created() -> None:
    global mem_students
    mem_students += 1
    if not _upstash_enabled():
        return
    try:
        val = await _upstash_request("incr/autopilot:students", [])
        if int(val or STUDENTS_SEED) < STUDENTS_SEED:
            await _upstash_request("set/autopilot:students/40", [])
    except Exception:
        return


async def record_lectures_completed(count: int) -> None:
    global mem_lectures
    if count <= 0:
        return
    mem_lectures += count
    if not _upstash_enabled():
        return
    try:
        await _upstash_request(f"incrby/autopilot:lectures/{count}", [])
    except Exception:
        return


def _job_redis_key(job_id: str) -> str:
    return f"{JOB_PREFIX}{job_id}"


async def save_job(job: JobState) -> None:
    if not _upstash_enabled():
        return
    try:
        key = _job_redis_key(job.id)
        payload = {
            "id": job.id,
            "status": job.status,
            "position": job.position,
            "dedupKey": job.dedup_key,
            "total": job.total,
            "processed": job.processed,
            "progress": job.progress,
            "createdAt": job.created_at,
            "logs": json.dumps(job.logs),
            "result": json.dumps(job.result) if job.result is not None else "",
        }
        for k, v in payload.items():
            await _redis_cmd("hset", key, k, v)
        await _redis_cmd("expire", key, 3600)
    except Exception:
        return


async def update_job_fields(job_id: str, **fields: Any) -> None:
    if not _upstash_enabled() or not fields:
        return
    try:
        key = _job_redis_key(job_id)
        for field_name, value in fields.items():
            stored = value
            if isinstance(value, (list, dict)):
                stored = json.dumps(value)
            await _redis_cmd("hset", key, field_name, stored)
        await _redis_cmd("expire", key, 3600)
    except Exception:
        return


def _coerce_job(data: dict[str, Any]) -> JobState | None:
    if not data or not data.get("id"):
        return None

    logs_raw = data.get("logs") or "[]"
    result_raw = data.get("result") or ""
    try:
        logs = json.loads(logs_raw)
    except Exception:
        logs = []
    try:
        result = json.loads(result_raw) if result_raw else None
    except Exception:
        result = None

    return JobState(
        id=str(data.get("id")),
        status=str(data.get("status", "queued")),
        position=int(data.get("position", 0) or 0),
        dedup_key=str(data.get("dedupKey", "")),
        logs=logs if isinstance(logs, list) else [],
        total=int(data.get("total", 0) or 0),
        processed=int(data.get("processed", 0) or 0),
        progress=int(data.get("progress", 0) or 0),
        created_at=int(data.get("createdAt", now_ms()) or now_ms()),
        result=result if isinstance(result, dict) else None,
    )


async def load_job(job_id: str) -> JobState | None:
    if not _upstash_enabled():
        return None
    try:
        data = await _redis_cmd("hgetall", _job_redis_key(job_id))
        if not isinstance(data, dict):
            return None
        return _coerce_job(data)
    except Exception:
        return None


async def delete_job(job_id: str) -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("del", _job_redis_key(job_id))
    except Exception:
        return


async def enqueue_job_id(job_id: str) -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("rpush", QUEUE_KEY, job_id)
        await _redis_cmd("expire", QUEUE_KEY, 14400)
    except Exception:
        return


async def remove_job_from_queue(job_id: str) -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("lrem", QUEUE_KEY, 0, job_id)
    except Exception:
        return


async def load_queued_job_ids() -> list[str]:
    if not _upstash_enabled():
        return []
    try:
        items = await _redis_cmd("lrange", QUEUE_KEY, 0, -1)
        if isinstance(items, list):
            return [str(item) for item in items]
        return []
    except Exception:
        return []


async def clear_queue() -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("del", QUEUE_KEY)
    except Exception:
        return


async def set_dedup_key(dedup_key: str, job_id: str) -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("set", f"{DEDUP_PREFIX}{dedup_key}", job_id)
    except Exception:
        return


async def get_dedup_key(dedup_key: str) -> str | None:
    if not _upstash_enabled():
        return None
    try:
        value = await _redis_cmd("get", f"{DEDUP_PREFIX}{dedup_key}")
        return str(value) if value else None
    except Exception:
        return None


async def delete_dedup_key(dedup_key: str) -> None:
    if not _upstash_enabled():
        return
    try:
        await _redis_cmd("del", f"{DEDUP_PREFIX}{dedup_key}")
    except Exception:
        return


async def restore_queue_from_redis() -> None:
    queued_ids = await load_queued_job_ids()
    if not queued_ids:
        return

    failed_result = {
        "success": False,
        "error": "Server was restarted. Please resubmit your request.",
    }

    for job_id in queued_ids:
        restored = await load_job(job_id)
        if not restored:
            continue
        restored.status = "failed"
        restored.result = failed_result
        jobs[job_id] = restored
        await update_job_fields(job_id, status="failed", result=failed_result)
        if restored.dedup_key:
            await delete_dedup_key(restored.dedup_key)

    await clear_queue()


def _safe_compare(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode("utf-8"), right.encode("utf-8"))


def _validate_admin(password: str | None) -> None:
    if not ADMIN_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="Admin access is not configured (ADMIN_PASSWORD not set in env).",
        )
    provided = password or ""
    if not provided or not _safe_compare(provided, ADMIN_PASSWORD):
        raise HTTPException(status_code=401, detail="Unauthorized.")


async def enforce_submit_rate_limit(client_ip: str) -> None:
    now = now_ms()
    cutoff = now - SUBMIT_WINDOW_MS
    async with submit_rate_lock:
        bucket = submit_rate.get(client_ip, [])
        bucket = [ts for ts in bucket if ts >= cutoff]
        if len(bucket) >= SUBMIT_MAX:
            raise HTTPException(
                status_code=429,
                detail="Too many requests. Even VTU has a rate limit (sort of).",
            )
        bucket.append(now)
        submit_rate[client_ip] = bucket


async def expire_job_later(job_id: str, seconds: int = 3600) -> None:
    await asyncio.sleep(seconds)
    async with state_lock:
        jobs.pop(job_id, None)
        sse_connections.pop(job_id, None)
    await delete_job(job_id)


async def maybe_start_jobs() -> None:
    global active_jobs

    while True:
        to_reposition: list[tuple[str, int]] = []
        picked: tuple[str, JobConfig] | None = None

        async with state_lock:
            if active_jobs >= runtime_config.max_concurrent or not queue:
                return

            picked = queue.pop(0)
            job_id, _ = picked
            job = jobs.get(job_id)
            if not job:
                continue

            for idx, (queued_job_id, _) in enumerate(queue, start=1):
                queued_job = jobs.get(queued_job_id)
                if not queued_job:
                    continue
                queued_job.position = idx
                to_reposition.append((queued_job_id, idx))

            job.status = "processing"
            job.position = 0
            active_jobs += 1

        for queued_job_id, position in to_reposition:
            await push(queued_job_id, "queue_pos", {"position": position})

        if picked is None:
            continue

        picked_job_id, picked_config = picked
        await push(picked_job_id, "status", {"status": "processing"})
        await remove_job_from_queue(picked_job_id)
        await update_job_fields(picked_job_id, status="processing", position=0)
        asyncio.create_task(run_job(picked_job_id, picked_config))


async def run_automation(
    config: JobConfig,
    on_progress: Callable[[str, dict[str, Any]], Awaitable[None]],
) -> dict[str, Any]:
    if not VTU_API_BASE_URL:
        return {
            "success": False,
            "error": "VTU_API_BASE_URL is not configured. Set it in your environment.",
        }

    base_candidates = api_base_candidates(VTU_API_BASE_URL)
    if not base_candidates:
        return {
            "success": False,
            "error": "VTU_API_BASE_URL is invalid. Set it to https://online.vtu.ac.in/api or /api/v1.",
        }

    completed = 0
    skipped = 0
    session_valid = False
    duration_cache: dict[int, int] = {}
    current_api_base = base_candidates[0]

    async with httpx.AsyncClient(timeout=30.0) as client:
        async def login() -> None:
            nonlocal current_api_base, session_valid

            last_error: str | None = None
            for candidate_base in base_candidates:
                for attempt in range(3):
                    try:
                        resp = await client.post(
                            f"{candidate_base}/auth/login",
                            json={"email": config.email, "password": config.password},
                        )
                        resp.raise_for_status()
                        current_api_base = candidate_base
                        name = (resp.json().get("data") or {}).get("name", config.email)
                        session_valid = True
                        await on_progress("log", {"text": f"Logged in as {name}", "level": "success"})
                        if candidate_base != base_candidates[0]:
                            await on_progress(
                                "log",
                                {
                                    "text": f"Switched API base to {candidate_base} after login failures.",
                                    "level": "warning",
                                },
                            )
                        return
                    except httpx.HTTPStatusError as exc:
                        status = exc.response.status_code
                        last_error = f"{status} on {candidate_base}/auth/login"
                        transient = status in {429, 500, 502, 503, 504}
                        if transient and attempt < 2:
                            await asyncio.sleep(max(runtime_config.retry_delay_ms, 0) / 1000)
                            continue
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = str(exc)
                        if attempt < 2:
                            await asyncio.sleep(max(runtime_config.retry_delay_ms, 0) / 1000)
                            continue
                        break

            raise RuntimeError(
                "Login failed after retries across API base candidates "
                f"{', '.join(base_candidates)}. Last error: {last_error or 'unknown error'}"
            )

        async def request(method: str, path: str, data: dict[str, Any] | None = None, retry: bool = True) -> httpx.Response:
            nonlocal session_valid
            if not session_valid:
                await login()
            try:
                response = await client.request(
                    method,
                    f"{current_api_base}{path}",
                    json=data,
                )
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if retry and status in (401, 403, 419):
                    session_valid = False
                    await login()
                    return await request(method, path, data, retry=False)
                if retry and status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(max(runtime_config.retry_delay_ms, 0) / 1000)
                    return await request(method, path, data, retry=False)
                raise

        await on_progress("phase", {"message": "Sneaking past VTU login..."})

        try:
            await login()
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            await on_progress("failed", {"message": msg})
            return {"success": False, "error": msg}

        await on_progress("phase", {"message": "Loading course structure..."})

        try:
            course_resp = await request("GET", f"/student/my-courses/{config.course_slug}")
            course_data = (course_resp.json() or {}).get("data") or {}
            course_title = course_data.get("title", config.course_slug)
            lectures: list[dict[str, Any]] = []
            for lesson in course_data.get("lessons", []):
                for lecture in lesson.get("lectures", []):
                    lectures.append(
                        {
                            "id": lecture.get("id"),
                            "title": lecture.get("title", "Untitled Lecture"),
                            "is_completed": lecture.get("is_completed") is True,
                        }
                    )
            await on_progress(
                "log",
                {
                    "text": f'"{course_title}" - {len(lectures)} lectures found',
                    "level": "success",
                },
            )
            await on_progress("course_info", {"title": course_title, "total": len(lectures)})
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                msg = "Course not found. Double-check the slug."
            else:
                msg = f"Failed to load course: {exc}"
            await on_progress("failed", {"message": msg})
            return {"success": False, "error": msg}
        except Exception as exc:  # noqa: BLE001
            msg = f"Failed to load course: {exc}"
            await on_progress("failed", {"message": msg})
            return {"success": False, "error": msg}

        total = len(lectures)
        await on_progress("phase", {"message": f"Processing {total} lectures in batches..."})

        pending: list[tuple[dict[str, Any], int]] = []
        for idx, lecture in enumerate(lectures, start=1):
            if lecture["is_completed"]:
                skipped += 1
                await on_progress(
                    "lecture_done",
                    {
                        "idx": idx,
                        "total": total,
                        "title": lecture["title"],
                        "status": "skip",
                        "reason": "Already completed",
                        "completed": completed,
                        "skipped": skipped,
                    },
                )
            else:
                pending.append((lecture, idx))

        async def try_once(item: tuple[dict[str, Any], int]) -> str:
            nonlocal completed, skipped

            lecture, idx = item
            lecture_id = lecture["id"]
            try:
                if lecture_id not in duration_cache:
                    details = await request(
                        "GET", f"/student/my-courses/{config.course_slug}/lectures/{lecture_id}"
                    )
                    duration_cache[lecture_id] = parse_duration(
                        ((details.json() or {}).get("data") or {}).get("duration")
                    )

                secs = duration_cache[lecture_id]
                if not secs:
                    skipped += 1
                    await on_progress(
                        "lecture_done",
                        {
                            "idx": idx,
                            "total": total,
                            "title": lecture["title"],
                            "status": "skip",
                            "reason": "VTU reported zero duration - no video content.",
                            "completed": completed,
                            "skipped": skipped,
                        },
                    )
                    return "skip"

                delay_sec = max(runtime_config.request_delay_ms, 0) / 1000
                if delay_sec > 0:
                    await asyncio.sleep(delay_sec)

                progress_resp = await request(
                    "POST",
                    f"/student/my-courses/{config.course_slug}/lectures/{lecture_id}/progress",
                    data={
                        "current_time_seconds": secs,
                        "total_duration_seconds": secs,
                        "seconds_just_watched": secs,
                    },
                )
                payload = (progress_resp.json() or {}).get("data") or {}
                percent = payload.get("percent")
                is_completed = payload.get("is_completed")

                if percent == 100 and is_completed is True:
                    completed += 1
                    await on_progress(
                        "lecture_done",
                        {
                            "idx": idx,
                            "total": total,
                            "title": lecture["title"],
                            "status": "done",
                            "completed": completed,
                            "skipped": skipped,
                        },
                    )
                    return "done"

                return "retry"
            except Exception:  # noqa: BLE001
                return "retry"

        for _ in range(config.max_attempts):
            if not pending:
                break

            next_pending: list[tuple[dict[str, Any], int]] = []
            for start in range(0, len(pending), config.batch_size):
                batch = pending[start : start + config.batch_size]
                results = await asyncio.gather(*(try_once(item) for item in batch))
                for batch_item, status in zip(batch, results, strict=True):
                    if status == "retry":
                        next_pending.append(batch_item)

            pending = next_pending

        if pending:
            await on_progress(
                "phase",
                {
                    "message": f"{len(pending)} lecture(s) could not be completed after {config.max_attempts} rounds."
                },
            )
            for lecture, idx in pending:
                await on_progress(
                    "lecture_done",
                    {
                        "idx": idx,
                        "total": total,
                        "title": lecture["title"],
                        "status": "maxed",
                        "reason": f"Did not reach 100% after {config.max_attempts} rounds.",
                        "completed": completed,
                        "skipped": skipped,
                    },
                )

    return {"success": True, "completed": completed, "skipped": skipped, "total": total}


async def run_job(job_id: str, config: JobConfig) -> None:
    global active_jobs

    async def on_progress(event: str, data: dict[str, Any]) -> None:
        persist_fields: dict[str, Any] = {}
        async with state_lock:
            job = jobs.get(job_id)
            if not job:
                return

            job.logs.append({"type": event, "data": data, "ts": now_ms()})
            if len(job.logs) > 200:
                job.logs.pop(0)

            if event == "course_info":
                job.total = int(data.get("total", 0))
                persist_fields["total"] = job.total
            if event == "lecture_done":
                job.processed = int(data.get("completed", job.processed))
                if job.total > 0:
                    job.progress = round((job.processed / job.total) * 100)
                persist_fields["processed"] = job.processed
                persist_fields["progress"] = job.progress

        if persist_fields:
            await update_job_fields(job_id, **persist_fields)

        await push(job_id, event, data)

    result = await run_automation(config, on_progress)

    dedup_to_clear = f"{config.email.lower()}:{config.course_slug.lower()}"
    final_fields: dict[str, Any] = {}

    async with state_lock:
        job = jobs.get(job_id)
        if job:
            job.status = "done" if result.get("success") else "failed"
            job.result = result
            final_fields = {
                "status": job.status,
                "result": result,
                "logs": job.logs,
                "total": job.total,
                "processed": job.processed,
                "progress": job.progress,
            }
        else:
            final_fields = {"status": "failed", "result": result}
        active_jobs = max(active_jobs - 1, 0)
        active_job_keys.pop(dedup_to_clear, None)

    await delete_dedup_key(dedup_to_clear)
    await update_job_fields(job_id, **final_fields)

    if result.get("success"):
        await record_lectures_completed(int(result.get("completed", 0)))
        done_payload = dict(result)
        done_payload["stats"] = await get_stats()
        await push(job_id, "done", done_payload)
    else:
        await push(job_id, "failed", {"message": result.get("error", "Unexpected server error")})

    config.password = ""
    config.email = ""

    asyncio.create_task(expire_job_later(job_id))
    await maybe_start_jobs()


async def submit_job(payload: SubmitPayload) -> dict[str, Any]:
    email = payload.email.strip()
    password = payload.password
    course_slug = extract_course_slug(payload.courseSlug)
    batch_size = payload.batchSize if payload.batchSize is not None else runtime_config.batch_size
    max_attempts = (
        payload.maxAttempts if payload.maxAttempts is not None else runtime_config.max_attempts
    )

    if "@" not in email or email.startswith("@"):
        raise HTTPException(status_code=400, detail="Invalid email address.")
    if not sanitize_slug(course_slug):
        raise HTTPException(
            status_code=400,
            detail="Invalid course slug or URL. Provide a slug or a valid VTU learning URL.",
        )
    if not (1 <= int(batch_size) <= 50):
        raise HTTPException(status_code=400, detail="batchSize must be between 1 and 50.")
    if not (1 <= int(max_attempts) <= 500):
        raise HTTPException(status_code=400, detail="maxAttempts must be between 1 and 500.")

    dedup_key = f"{email.lower()}:{course_slug.lower()}"
    redis_existing_id = await get_dedup_key(dedup_key)

    async with state_lock:
        existing_id = active_job_keys.get(dedup_key) or redis_existing_id
        if existing_id:
            existing_job = jobs.get(existing_id)
            if existing_job and existing_job.status in {"queued", "processing"}:
                return {
                    "jobId": existing_id,
                    "position": existing_job.position,
                    "existing": True,
                }

    if redis_existing_id:
        existing_job = await load_job(redis_existing_id)
        if existing_job and existing_job.status in {"queued", "processing"}:
            async with state_lock:
                jobs[redis_existing_id] = existing_job
            return {
                "jobId": redis_existing_id,
                "position": existing_job.position,
                "existing": True,
            }

    async with state_lock:
        if dedup_key in active_job_keys:
            existing_id = active_job_keys[dedup_key]
            existing_job = jobs.get(existing_id)
            if existing_job and existing_job.status in {"queued", "processing"}:
                return {
                    "jobId": existing_id,
                    "position": existing_job.position,
                    "existing": True,
                }

        job_id = str(uuid.uuid4())
        position = len(queue) + active_jobs + 1

        jobs[job_id] = JobState(
            id=job_id,
            status="queued",
            position=position,
            dedup_key=dedup_key,
        )
        active_job_keys[dedup_key] = job_id
        queue.append(
            (
                job_id,
                JobConfig(
                    email=email,
                    password=password,
                    course_slug=course_slug,
                    batch_size=int(batch_size),
                    max_attempts=int(max_attempts),
                ),
            )
        )

    asyncio.create_task(record_job_created())
    asyncio.create_task(save_job(jobs[job_id]))
    asyncio.create_task(enqueue_job_id(job_id))
    asyncio.create_task(set_dedup_key(dedup_key, job_id))

    await maybe_start_jobs()
    return {"jobId": job_id, "position": position}


@app.get("/")
async def home() -> FileResponse:
    return FileResponse("frontend/index.html")


@app.post("/api/submit")
async def api_submit(payload: SubmitPayload, request: Request) -> dict[str, Any]:
    client_ip = request.client.host if request.client else "unknown"
    await enforce_submit_rate_limit(client_ip)
    return await submit_job(payload)


@app.post("/api/jobs")
async def api_jobs(payload: SubmitPayload, request: Request) -> dict[str, Any]:
    client_ip = request.client.host if request.client else "unknown"
    await enforce_submit_rate_limit(client_ip)
    return await submit_job(payload)


@app.get("/api/queue")
async def api_queue() -> dict[str, int]:
    async with state_lock:
        queued = len(queue)
        processing = active_jobs
    return {"queued": queued, "processing": processing, "total": queued + processing}


@app.get("/api/notification")
async def api_notification() -> dict[str, Any]:
    return notification_state


@app.get("/api/stats")
async def api_stats() -> dict[str, Any]:
    return await get_stats()


@app.get("/api/jobs/{job_id}")
async def api_job_status(job_id: str) -> dict[str, Any]:
    if not is_uuid_v4(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format.")
    async with state_lock:
        job = jobs.get(job_id)
    if not job:
        job = await load_job(job_id)
        if job:
            async with state_lock:
                jobs[job_id] = job
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")
    return snapshot(job)


@app.get("/api/admin/config")
async def api_admin_config(
    password: str | None = None,
    maxConcurrent: int | None = None,
    batchSize: int | None = None,
    maxAttempts: int | None = None,
    retryDelay: int | None = None,
    requestDelay: int | None = None,
) -> dict[str, Any]:
    _validate_admin(password)

    updates: dict[str, int] = {}
    incoming = {
        "maxConcurrent": maxConcurrent,
        "batchSize": batchSize,
        "maxAttempts": maxAttempts,
        "retryDelay": retryDelay,
        "requestDelay": requestDelay,
    }
    for key, value in incoming.items():
        if value is None:
            continue
        min_v, max_v = CONFIG_BOUNDS[key]
        if value < min_v or value > max_v:
            raise HTTPException(
                status_code=400,
                detail=f'"{key}" must be between {min_v} and {max_v}.',
            )
        updates[key] = value

    if updates:
        runtime_config.max_concurrent = updates.get("maxConcurrent", runtime_config.max_concurrent)
        runtime_config.batch_size = updates.get("batchSize", runtime_config.batch_size)
        runtime_config.max_attempts = updates.get("maxAttempts", runtime_config.max_attempts)
        runtime_config.retry_delay_ms = updates.get("retryDelay", runtime_config.retry_delay_ms)
        runtime_config.request_delay_ms = updates.get("requestDelay", runtime_config.request_delay_ms)

    return {
        "config": {
            "maxConcurrent": runtime_config.max_concurrent,
            "batchSize": runtime_config.batch_size,
            "maxAttempts": runtime_config.max_attempts,
            "retryDelay": runtime_config.retry_delay_ms,
            "requestDelay": runtime_config.request_delay_ms,
        },
        "bounds": CONFIG_BOUNDS,
    }


@app.get("/api/admin/monitor")
async def api_admin_monitor(password: str | None = None) -> dict[str, Any]:
    _validate_admin(password)

    async with state_lock:
        processing: list[dict[str, str]] = []
        for job_id, job in jobs.items():
            if job.status != "processing":
                continue
            email, _, course = job.dedup_key.partition(":")
            processing.append({"jobId": job_id, "email": email or "?", "course": course or "?"})

        queued: list[dict[str, Any]] = []
        for i, (job_id, cfg) in enumerate(queue, start=1):
            queued.append(
                {
                    "num": i,
                    "jobId": job_id,
                    "email": cfg.email or "?",
                    "course": cfg.course_slug or "?",
                }
            )

    if not processing and not queued:
        return {"message": "No active or queued jobs.", "processing": [], "queued": []}

    return {
        "activeJobs": len(processing),
        "queueLength": len(queued),
        "processing": processing,
        "queued": queued,
    }


@app.get("/api/admin/notification")
async def api_admin_notification(
    password: str | None = None,
    message: str | None = None,
    disabled: str | None = None,
) -> dict[str, Any]:
    _validate_admin(password)

    updates: dict[str, Any] = {}
    if message is not None:
        if len(message) > 500:
            raise HTTPException(
                status_code=400,
                detail="Notification message must be 500 characters or less.",
            )
        updates["message"] = message

    if disabled is not None:
        norm = disabled.strip().lower()
        if norm not in {"true", "false", "1", "0", "yes", "no"}:
            raise HTTPException(status_code=400, detail='"disabled" must be true or false.')
        updates["disabled"] = norm in {"true", "1", "yes"}

    if updates:
        notification_state.update(updates)

    return {"notification": notification_state}


@app.get("/api/status/{job_id}")
async def api_status_stream(job_id: str) -> StreamingResponse:
    return await _stream_job(job_id)


@app.get("/api/jobs/{job_id}/stream")
async def api_jobs_stream(job_id: str) -> StreamingResponse:
    return await _stream_job(job_id)


async def _stream_job(job_id: str) -> StreamingResponse:
    if not is_uuid_v4(job_id):
        raise HTTPException(status_code=400, detail="Invalid job ID format.")

    async with state_lock:
        job = jobs.get(job_id)
    if not job:
        job = await load_job(job_id)
        if job:
            async with state_lock:
                jobs[job_id] = job
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or expired.")

    async def event_stream() -> AsyncIterator[str]:
        local_queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=100)
        async with state_lock:
            if job_id not in sse_connections:
                sse_connections[job_id] = set()
            sse_connections[job_id].add(local_queue)
            current = jobs.get(job_id)

        if current is None:
            yield sse_chunk("failed", {"message": "Job was removed."})
            return

        yield sse_chunk("snapshot", snapshot(current))

        if current.status in {"done", "failed"}:
            async with state_lock:
                sse_connections.get(job_id, set()).discard(local_queue)
            return

        try:
            while True:
                try:
                    event, data = await asyncio.wait_for(local_queue.get(), timeout=20.0)
                    yield sse_chunk(event, data)
                    if event in {"done", "failed"}:
                        break
                except TimeoutError:
                    yield ":ping\n\n"
        finally:
            async with state_lock:
                sse_connections.get(job_id, set()).discard(local_queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.on_event("startup")
async def on_startup() -> None:
    await ensure_seed()
    await restore_queue_from_redis()
