# VTU Automate (FastAPI)

This is a FastAPI-based VTU Automate service and dashboard.

## Why this application is built

The app is built to reduce repetitive manual effort while marking VTU lecture progress. Instead of opening every lecture and waiting, you can submit one job and track everything in real time.

Main goals:

- Keep the same queue + SSE flow as the Express version
- Provide a clean UI to submit jobs and monitor progress
- Support full VTU learning links and plain slugs
- Keep setup simple for local execution

## Features

- FastAPI backend with queue processing
- Dedup for active jobs using `email + courseSlug`
- Real-time event stream (SSE)
- Optional Upstash Redis-backed stats (`/api/stats`) with in-memory fallback
- Submit rate limiting (5 submissions per 15 minutes per IP)
- SEO ready: meta/OpenGraph/Twitter tags, JSON-LD, `robots.txt`, `sitemap.xml`, and a 1200x630 share banner
- Dark vibrant frontend with responsive design
- Accepts either:
  - `1-fiber-optic-communication-technology`
  - `https://online.vtu.ac.in/student/learning/1-fiber-optic-communication-technology`

## 1) Setup

Requirements:

- Python 3.11+
- `uv` recommended

Install dependencies:

```bash
uv sync
```

Create your environment file:

1. Copy `.env.example` to `.env`
2. Edit values as needed

Example `.env`:

```env
VTU_API_BASE_URL=https://online.vtu.ac.in/api/v1
VTU_BATCH_SIZE=10
VTU_MAX_ATTEMPTS=100
MAX_CONCURRENT=2
CORS_ORIGIN=*
SITE_URL=https://vtu-automate.fastapicloud.dev
GITHUB_URL=https://github.com/vikas-bhat-d/vtu-course-automation
KV_REST_API_URL=https://your-db.upstash.io
KV_REST_API_TOKEN=your-rest-api-token
```

Run server:

```bash
uv run python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

Open:

- `http://127.0.0.1:8000` for UI
- `http://127.0.0.1:8000/docs` for API docs

## 2) How to use this application

1. Open the web UI.
2. Enter VTU email and password.
3. Paste full VTU learning URL or slug in the course field.
4. Click `Submit Job`.
5. Optionally tune `Batch Size` and `Max Attempts`.
6. Watch live queue, status, logs, and progress.
7. Save the Job ID if you want to reconnect later.
8. To reconnect, paste Job ID and click `Track Existing Job`.

## 3) How link to slug extraction works

If input is a URL, backend extracts the slug from the path.

Example:

- Input: `https://online.vtu.ac.in/student/learning/1-fiber-optic-communication-technology`
- Extracted slug: `1-fiber-optic-communication-technology`

If you provide only slug, it is used directly.

## 4) Backend flow

1. Validate user input.
2. Convert URL to slug when needed.
3. Deduplicate active job by `email + slug`.
4. Queue or process immediately based on `MAX_CONCURRENT`.
5. Stream events:
   - `snapshot`, `status`, `queue_pos`, `phase`, `log`, `course_info`, `lecture_done`, `done`, `failed`
6. Expire completed/failed jobs after 1 hour.

## 5) API parity with vtu-automate

- `POST /api/submit` and `POST /api/jobs` accept:
  - `email`, `password`, `courseSlug`
  - optional `batchSize`, `maxAttempts`
- `GET /api/jobs/{job_id}` polling status
- `GET /api/jobs/{job_id}/stream` and `GET /api/status/{job_id}` SSE
- `GET /api/stats` returns:
  - `studentsHelped`, `lecturesCompleted`, `githubUrl`
- `GET /api/queue` queue depth
- `GET /api/notification` public banner state

## 6) Notes

- Job state is in-memory and resets on server restart.
- Credentials are cleared from job config after run.
- Upstash Redis is optional and used for shared public counters.
- Missing `VTU_API_BASE_URL` causes a clear failure message.
- `SITE_URL` drives the absolute links in `robots.txt` and `sitemap.xml`. The
  canonical/OG/Twitter URLs in `frontend/index.html` are hardcoded to the same
  domain — update both if you deploy elsewhere.
- Regenerate the share banner after a branding change with
  `uv run --with pillow python scripts/generate_og_image.py` (Pillow is only
  needed for this one-off script, not at runtime).
