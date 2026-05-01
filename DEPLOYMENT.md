# FileForge Deployment Guide

## 1) Pre-Deploy Checklist

- Use Node.js LTS (recommended: v22.x).
- Install and run Redis (required for queue mode).
- Ensure `ffmpeg.exe` is accessible:
  - `C:\xampp\htdocs\int\convetor2\node_modules\ffmpeg-static\ffmpeg.exe`
- If Windows Security blocks FFmpeg (`spawn EPERM`), allow these apps:
  - `C:\Program Files\nodejs\node.exe`
  - `C:\xampp\htdocs\int\convetor2\node_modules\ffmpeg-static\ffmpeg.exe`
- Create `.env` from `.env.example`.
- Run smoke tests:
  - `npm run test:smoke`

## 2) Environment Setup

Copy the template and edit values:

```powershell
Copy-Item .\deploy\.env.production.example .\.env
```

Then update `.env` values for your server.

Template content:

```env
NODE_ENV=production
PORT=3000
QUEUE_ENABLED=true
REDIS_URL=redis://127.0.0.1:6379
CONVERSION_QUEUE_NAME=conversion-jobs
WORKER_CONCURRENCY=10
QUEUE_MAX_IN_FLIGHT=1000
RATE_LIMIT_PER_MINUTE=300
OCR_MODE=cloud-first
GOOGLE_VISION_API_KEY=your_google_vision_api_key_here
```

## 3) Install Dependencies

```powershell
npm ci
```

## 4) Start in Production (PM2)

```powershell
npm run prod:start:api
npm run prod:start:worker
```

Useful PM2 commands:

```powershell
npm run prod:status
npm run prod:logs
npm run prod:restart
npm run prod:stop
```

## 5) Queue API Flow

- `POST /convert` -> returns `{ success: true, status: "queued", jobId }`
- `GET /jobs/:id` -> check job status/progress/result
- `GET /jobs/:id/download` -> redirect to output file after completion

## 6) Verify Runtime Health

```powershell
curl.exe -sS http://localhost:3000/health
```

Healthy output should include:

- `"ok": true`
- `"degraded": false`
- `diagnostics.runtime.spawnEpermDetected: false`

## 7) Reverse Proxy + TLS (Recommended)

Put app behind Nginx/Caddy/Apache:

- Public HTTPS endpoint -> `http://localhost:3000`
- Enable request size limits for uploads
- Enable TLS cert auto-renewal
- Sample Nginx config is available at `deploy/nginx-fileforge.conf`

## 8) Ops Notes

- `uploads/` and `downloads/` can grow quickly. Add scheduled cleanup.
- Keep logs rotated (`server.out.log`, `server.err.log`, PM2 logs).
- Re-run `npm run test:smoke` after dependency or OS security changes.
