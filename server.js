require("dotenv").config();
const express = require("express");
const multer = require("multer");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const path = require("path");
const fs = require("fs/promises");
const fsSync = require("fs");
const sharp = require("sharp");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const pdfParse = require("pdf-parse");
const mammoth = require("mammoth");
const { Document, Packer, Paragraph } = require("docx");
const ffmpeg = require("fluent-ffmpeg");
const ffmpegPath = require("ffmpeg-static");
const IORedis = require("ioredis");
const { Queue } = require("bullmq");
const { createWorker } = require("tesseract.js");
const { spawn, spawnSync } = require("child_process");

let resolvedFfmpegPath = null;
let ffmpegProbeError = null;
let processSpawnProbeError = null;
let lastSpawnEpermMessage = null;
let lastSpawnEpermAt = null;

function markSpawnEperm(message) {
  lastSpawnEpermMessage = String(message || "spawn EPERM");
  lastSpawnEpermAt = new Date().toISOString();
}

function probeChildProcessSpawn() {
  try {
    const probe = spawnSync(process.execPath, ["-e", "process.exit(0)"], {
      windowsHide: true,
      stdio: "ignore",
      timeout: 4000
    });
    if (probe.error) {
      return probe.error.message || "Unknown child-process spawn error";
    }
    if (probe.status !== 0) {
      return `Child-process probe exited with code ${probe.status}`;
    }
    return null;
  } catch (err) {
    return err?.message || "Unknown child-process spawn exception";
  }
}

function canExecuteFfmpeg(binary) {
  try {
    const probe = spawnSync(binary, ["-version"], {
      windowsHide: true,
      stdio: "ignore",
      timeout: 4000
    });
    if (probe.error) {
      return false;
    }
    return probe.status === 0;
  } catch (_err) {
    return false;
  }
}

function resolveFfmpegBinary() {
  if (canExecuteFfmpeg("ffmpeg")) {
    return "ffmpeg";
  }
  // Prefer bundled binary only when system ffmpeg is unavailable.
  // Some Windows setups block ffmpeg-static executable with EPERM.
  if (ffmpegPath && canExecuteFfmpeg(ffmpegPath)) {
    return ffmpegPath;
  }
  return null;
}

resolvedFfmpegPath = resolveFfmpegBinary();
processSpawnProbeError = probeChildProcessSpawn();
if (resolvedFfmpegPath) {
  ffmpeg.setFfmpegPath(resolvedFfmpegPath);
} else {
  ffmpegProbeError = processSpawnProbeError
    ? `FFmpeg unavailable because child-process spawning failed (${processSpawnProbeError}).`
    : "FFmpeg executable is not accessible (EPERM or missing). Video/audio conversions are unavailable.";
}

const app = express();
const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const uploadDir = path.join(ROOT, "uploads");
const downloadDir = path.join(ROOT, ".runtime-generated");
const QUEUE_NAME = String(process.env.CONVERSION_QUEUE_NAME || "conversion-jobs").trim();
const REDIS_URL = String(process.env.REDIS_URL || "redis://127.0.0.1:6379").trim();
const QUEUE_ENABLED = String(process.env.QUEUE_ENABLED || "false").toLowerCase() === "true";
const QUEUE_MAX_IN_FLIGHT = Math.max(1, Number.parseInt(process.env.QUEUE_MAX_IN_FLIGHT || "1000", 10) || 1000);

let redisConnection = null;
let conversionQueue = null;
const failureTracker = {
  totalFailures: 0,
  consecutiveFailures: 0,
  lastFailureAt: null,
  lastFailureMessage: null,
  byRouteKey: new Map()
};
const MAX_RETRY_ATTEMPTS = Math.max(1, Number.parseInt(process.env.CONVERSION_MAX_RETRIES || "2", 10) || 2);
const RETRY_BASE_DELAY_MS = Math.max(100, Number.parseInt(process.env.CONVERSION_RETRY_BASE_DELAY_MS || "350", 10) || 350);
const FAILURE_COOLDOWN_MS = Math.max(1000, Number.parseInt(process.env.FAILURE_COOLDOWN_MS || "45000", 10) || 45000);
const FAILURE_THRESHOLD = Math.max(3, Number.parseInt(process.env.FAILURE_THRESHOLD || "6", 10) || 6);

function makeRouteKey(inputExt, outputExt) {
  return `${String(inputExt || "").toLowerCase()}->${String(outputExt || "").toLowerCase()}`;
}

function markFailure(inputExt, outputExt, message) {
  const key = makeRouteKey(inputExt, outputExt);
  const now = Date.now();
  const current = failureTracker.byRouteKey.get(key) || { count: 0, lastAt: 0, lastMessage: null };
  current.count += 1;
  current.lastAt = now;
  current.lastMessage = String(message || "unknown conversion failure");
  failureTracker.byRouteKey.set(key, current);
  failureTracker.totalFailures += 1;
  failureTracker.consecutiveFailures += 1;
  failureTracker.lastFailureAt = new Date(now).toISOString();
  failureTracker.lastFailureMessage = current.lastMessage;
}

function markSuccess(inputExt, outputExt) {
  const key = makeRouteKey(inputExt, outputExt);
  const current = failureTracker.byRouteKey.get(key);
  if (current) {
    current.count = 0;
    failureTracker.byRouteKey.set(key, current);
  }
  failureTracker.consecutiveFailures = 0;
}

function isRouteTemporarilyBlocked(inputExt, outputExt) {
  const key = makeRouteKey(inputExt, outputExt);
  const current = failureTracker.byRouteKey.get(key);
  if (!current || current.count < FAILURE_THRESHOLD) {
    return false;
  }
  return Date.now() - Number(current.lastAt || 0) < FAILURE_COOLDOWN_MS;
}

function isRetryableError(error) {
  const msg = String(error?.message || "").toLowerCase();
  return msg.includes("spawn eperm") || msg.includes("ffmpeg failed") || msg.includes("timed out") || msg.includes("econnreset");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getQueueConnection() {
  if (!redisConnection) {
    redisConnection = new IORedis(REDIS_URL, {
      maxRetriesPerRequest: null,
      enableReadyCheck: true
    });
  }
  return redisConnection;
}

function getConversionQueue() {
  if (!QUEUE_ENABLED) {
    throw new Error("Queue mode disabled. Set QUEUE_ENABLED=true to enqueue /convert jobs.");
  }
  if (!conversionQueue) {
    conversionQueue = new Queue(QUEUE_NAME, {
      connection: getQueueConnection(),
      defaultJobOptions: {
        removeOnComplete: 500,
        removeOnFail: 500,
        attempts: 1
      }
    });
  }
  return conversionQueue;
}

const ALLOWED = {
  jpg: ["png", "webp", "txt", "pdf", "docx"],
  jpeg: ["png", "webp", "txt", "pdf", "docx"],
  png: ["jpg", "webp", "txt", "pdf", "docx"],
  webp: ["jpg", "png"],
  pdf: ["txt", "docx", "pdf"],
  csv: ["xlsx"],
  xlsx: ["csv"],
  txt: ["pdf", "docx"],
  docx: ["txt"],
  mp4: ["mp3", "wav", "txt", "srt", "pdf", "docx"],
  mkv: ["mp3", "wav", "txt", "srt", "pdf", "docx"],
  avi: ["mp3", "wav", "txt", "srt", "pdf", "docx"],
  mp3: ["txt", "srt"],
  wav: ["txt", "srt"]
};

const VIDEO_EXTS = new Set(["mp4", "mkv", "avi"]);
const AUDIO_EXTS = new Set(["mp3", "wav"]);
const OCR_IMAGE_EXTS = new Set(["jpg", "jpeg", "png"]);
const MERGEABLE_IMAGE_EXTS = new Set(["jpg", "jpeg", "png", "webp"]);
let whisperPipeline = null;
let whisperLoadingPromise = null; // FIX: Prevent race condition
const pendingCleanupFiles = new Set();
let cleanupRetryTimer = null;
const OCR_MODE = String(process.env.OCR_MODE || "auto").toLowerCase();
const ASR_MODEL = String(process.env.ASR_MODEL || "Xenova/whisper-small").trim();
const EXT_ALIASES = {
  jfif: "jpg",
  jpe: "jpeg"
};
const MIME_TO_EXT = {
  "image/jpeg": "jpeg",
  "image/png": "png",
  "image/webp": "webp",
  "application/pdf": "pdf",
  "text/plain": "txt",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
  "text/csv": "csv",
  "audio/mpeg": "mp3",
  "audio/wav": "wav",
  "audio/x-wav": "wav",
  "video/mp4": "mp4",
  "video/x-msvideo": "avi",
  "video/x-matroska": "mkv"
};

// Utility: Extract file extension
function extOf(fileName) {
  return path.extname(fileName).replace(".", "").toLowerCase();
}

function normalizeInputExt(ext) {
  const raw = String(ext || "")
    .toLowerCase()
    .trim()
    .replace(/^\./, "")
    .replace(/;.*$/, "");
  return EXT_ALIASES[raw] || raw;
}

function detectInputExt(file) {
  const byName = normalizeInputExt(extOf(file?.originalname || ""));
  if (byName) {
    return byName;
  }
  const byMime = MIME_TO_EXT[String(file?.mimetype || "").toLowerCase()] || "";
  return normalizeInputExt(byMime);
}

// Utility: Remove file extension
function stripExt(fileName) {
  return fileName.replace(/\.[^/.]+$/, "");
}

// Utility: Sanitize filename
function sanitizeName(name) {
  const sanitized = name
    .replace(/[^a-zA-Z0-9-_]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  return sanitized || "converted-file";
}

function isSafeGeneratedName(fileName) {
  const name = String(fileName || "").trim();
  if (!name || name.includes("/") || name.includes("\\") || name.includes("..")) {
    return false;
  }
  return /^[a-zA-Z0-9._-]+$/.test(name);
}

// Utility: Safe file deletion
async function safeDelete(filePath) {
  try {
    await fs.unlink(filePath);
  } catch (_err) {
    // Ignore cleanup errors
  }
}

async function safeDeleteWithRetries(filePath, maxAttempts = 5, delayMs = 250) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await fs.unlink(filePath);
      return true;
    } catch (err) {
      const code = String(err?.code || "");
      if (code === "ENOENT") {
        return true;
      }
      if (attempt === maxAttempts) {
        return false;
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs * attempt));
    }
  }
  return false;
}

function scheduleCleanupRetry() {
  if (cleanupRetryTimer) {
    return;
  }
  cleanupRetryTimer = setTimeout(async () => {
    cleanupRetryTimer = null;
    const names = Array.from(pendingCleanupFiles);
    if (!names.length) {
      return;
    }
    for (const fileName of names) {
      const filePath = path.join(downloadDir, fileName);
      const deleted = await safeDeleteWithRetries(filePath, 8, 300);
      if (deleted) {
        pendingCleanupFiles.delete(fileName);
      }
    }
    if (pendingCleanupFiles.size) {
      scheduleCleanupRetry();
    }
  }, 5000);
}

async function queueCleanupFile(fileName) {
  const name = String(fileName || "").trim();
  if (!isSafeGeneratedName(name)) {
    return false;
  }
  const filePath = path.join(downloadDir, name);
  const deleted = await safeDeleteWithRetries(filePath, 8, 250);
  if (!deleted) {
    pendingCleanupFiles.add(name);
    scheduleCleanupRetry();
  } else {
    pendingCleanupFiles.delete(name);
  }
  return deleted;
}

// Utility: Ensure directories exist
async function ensureDirs() {
  await fs.mkdir(uploadDir, { recursive: true });
  await fs.mkdir(downloadDir, { recursive: true });
}

// Utility: Check file is not empty
async function ensureNonEmptyFile(filePath) {
  const st = await fs.stat(filePath);
  if (!st.isFile() || st.size <= 0) {
    throw new Error("Converted file is empty");
  }
}

// Utility: Validate format parameter
function isValidFormat(format) {
  return typeof format === "string" && /^[a-z0-9]+$/.test(format);
}

// Logger middleware
function logger(req, res, next) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${req.method} ${req.path}`);
  next();
}

// Configure multer storage
const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const unique = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    cb(null, `${unique}${path.extname(file.originalname).toLowerCase()}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    const inputExt = detectInputExt(file);
    if (!Object.prototype.hasOwnProperty.call(ALLOWED, inputExt)) {
      const suffix = inputExt ? `.${inputExt}` : "(unknown)";
      return cb(new Error(`Unsupported input format ${suffix}`));
    }
    cb(null, true);
  }
});

// Middleware
app.use(helmet({
  crossOriginResourcePolicy: false,
  // Keep security headers, but avoid CSP mismatches that can alter frontend rendering
  // between Apache and Node-served pages.
  contentSecurityPolicy: false
}));
app.use(logger);
app.use(rateLimit({
  windowMs: 60 * 1000,
  max: Number.parseInt(process.env.RATE_LIMIT_PER_MINUTE || "300", 10) || 300,
  standardHeaders: true,
  legacyHeaders: false
}));
app.use((req, res, next) => {
  // Allow local frontend (including XAMPP/file-based workflows) to call this API.
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }
  next();
});
app.use(express.json());
app.use(express.static(ROOT));

app.get("/download-once/:fileName", async (req, res) => {
  const fileName = String(req.params.fileName || "").trim();
  if (!isSafeGeneratedName(fileName)) {
    return res.status(400).json({ success: false, message: "Invalid file name" });
  }

  const filePath = path.join(downloadDir, fileName);
  try {
    await ensureNonEmptyFile(filePath);
  } catch (_err) {
    return res.status(404).json({ success: false, message: "File not found" });
  }

  try {
    const bytes = await fs.readFile(filePath);
    await queueCleanupFile(fileName);
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.setHeader("Content-Type", "application/octet-stream");
    return res.send(bytes);
  } catch (err) {
    return res.status(500).json({
      success: false,
      message: err?.message || "Failed to serve download"
    });
  }
});

// Routes
app.get("/health", (_req, res) => {
  const diagnostics = {
    childProcess: {
      spawnOk: !processSpawnProbeError,
      error: processSpawnProbeError || null
    },
    ffmpeg: {
      available: !!resolvedFfmpegPath,
      binary: resolvedFfmpegPath || null,
      error: ffmpegProbeError || null
    },
    runtime: {
      spawnEpermDetected: !!lastSpawnEpermAt,
      lastSpawnEpermAt,
      lastSpawnEpermMessage
    },
    queue: {
      enabled: QUEUE_ENABLED,
      name: QUEUE_NAME
    },
    selfHealing: {
      maxRetries: MAX_RETRY_ATTEMPTS,
      retryBaseDelayMs: RETRY_BASE_DELAY_MS,
      failureThreshold: FAILURE_THRESHOLD,
      failureCooldownMs: FAILURE_COOLDOWN_MS,
      totalFailures: failureTracker.totalFailures,
      consecutiveFailures: failureTracker.consecutiveFailures,
      lastFailureAt: failureTracker.lastFailureAt,
      lastFailureMessage: failureTracker.lastFailureMessage
    }
  };
  res.json({
    ok: true,
    service: "fileforge-converter",
    degraded:
      !diagnostics.childProcess.spawnOk ||
      !diagnostics.ffmpeg.available ||
      diagnostics.runtime.spawnEpermDetected,
    diagnostics
  });
});

app.post("/convert", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ success: false, message: "File missing" });
  }

  try {
    const format = String(req.body.format || "").toLowerCase().trim();
    const inputExt = detectInputExt(req.file);
    const allowedTargets = ALLOWED[inputExt] || [];

    // Validate format parameter
    if (!isValidFormat(format)) {
      await safeDelete(req.file.path);
      return res.status(400).json({
        success: false,
        message: "Invalid format parameter"
      });
    }

    // Check if conversion is supported
    if (!allowedTargets.includes(format)) {
      await safeDelete(req.file.path);
      return res.status(400).json({
        success: false,
        message: `.${inputExt} to .${format} conversion is not supported`
      });
    }

    if (QUEUE_ENABLED) {
      const queue = getConversionQueue();
      const counts = await queue.getJobCounts("waiting", "active", "delayed", "prioritized");
      const inFlight =
        Number(counts.waiting || 0) +
        Number(counts.active || 0) +
        Number(counts.delayed || 0) +
        Number(counts.prioritized || 0);
      if (inFlight >= QUEUE_MAX_IN_FLIGHT) {
        await safeDelete(req.file.path);
        return res.status(503).json({
          success: false,
          message: "Server busy, retry later"
        });
      }

      const job = await queue.add("convert", {
        inputPath: req.file.path,
        originalName: req.file.originalname,
        inputExt,
        outputExt: format,
        bitrate: String(req.body.bitrate || "").trim()
      });
      return res.status(202).json({
        success: true,
        status: "queued",
        jobId: job.id
      });
    }

    const result = await processQueuedConversion({
      inputPath: req.file.path,
      originalName: req.file.originalname,
      inputExt,
      outputExt: format,
      bitrate: String(req.body.bitrate || "").trim()
    });
    return res.json({
      success: true,
      filename: result.filename,
      url: `/download-once/${result.filename}`
    });
  } catch (error) {
    // Clean up on error
    if (req.file?.path) {
      await safeDelete(req.file.path);
    }
    console.error("Conversion error:", error);
    let message = error?.message || "Conversion failed";
    if (/spawn EPERM/i.test(message)) {
      markSpawnEperm(message);
      message =
        "Conversion failed because this Windows environment is blocking Node from starting helper tools (spawn EPERM). Allow node.exe and ffmpeg.exe in Windows Security or run terminal as Administrator, then restart the server.";
    }
    return res.status(500).json({
      success: false,
      message
    });
  }
});

app.get("/jobs/:id", async (req, res) => {
  try {
    const queue = getConversionQueue();
    const job = await queue.getJob(req.params.id);
    if (!job) {
      return res.status(404).json({ success: false, message: "Job not found" });
    }
    const state = await job.getState();
    const payload = {
      success: true,
      jobId: job.id,
      status: state,
      progress: job.progress || 0
    };
    if (state === "completed") {
      payload.result = job.returnvalue || null;
    }
    if (state === "failed") {
      payload.error = job.failedReason || "Job failed";
    }
    return res.json(payload);
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to fetch job"
    });
  }
});

app.get("/jobs/:id/download", async (req, res) => {
  try {
    const queue = getConversionQueue();
    const job = await queue.getJob(req.params.id);
    if (!job) {
      return res.status(404).json({ success: false, message: "Job not found" });
    }
    const state = await job.getState();
    if (state !== "completed" || !job.returnvalue?.filename) {
      return res.status(409).json({
        success: false,
        message: "Job is not completed yet"
      });
    }
    return res.redirect(job.returnvalue.url || `/downloads/${job.returnvalue.filename}`);
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to fetch download URL"
    });
  }
});

app.post("/images-to-pdf", upload.array("files", 50), async (req, res) => {
  const files = Array.isArray(req.files) ? req.files : [];
  if (files.length < 1) {
    return res.status(400).json({ success: false, message: "Select at least 1 image." });
  }
  if (files.length > 50) {
    await Promise.all(files.map((f) => safeDelete(f.path)));
    return res.status(400).json({ success: false, message: "You can upload maximum 50 images." });
  }

  const invalid = files.find((f) => !MERGEABLE_IMAGE_EXTS.has(detectInputExt(f)));
  if (invalid) {
    await Promise.all(files.map((f) => safeDelete(f.path)));
    return res.status(400).json({
      success: false,
      message: "Only jpg, jpeg, png, webp images are allowed for multi-image PDF."
    });
  }

  const baseName = sanitizeName(stripExt(files[0].originalname || "images"));
  const outputName = `${baseName}-bundle-${Date.now()}.pdf`;
  const outputPath = path.join(downloadDir, outputName);
  try {
    await imagesToPdf(files, outputPath);
    await ensureNonEmptyFile(outputPath);
    await Promise.all(files.map((f) => safeDelete(f.path)));
    return res.json({
      success: true,
      filename: outputName,
      url: `/download-once/${outputName}`
    });
  } catch (error) {
    await Promise.all(files.map((f) => safeDelete(f.path)));
    await safeDelete(outputPath);
    return res.status(500).json({
      success: false,
      message: error.message || "Failed to generate PDF from images"
    });
  }
});

app.post("/cleanup", async (req, res) => {
  try {
    const files = Array.isArray(req.body?.files) ? req.body.files : [];
    let deleted = 0;
    const failed = [];
    for (const file of files) {
      const fileName = String(file || "").trim();
      if (!isSafeGeneratedName(fileName)) {
        failed.push({ file: fileName, reason: "invalid_name" });
        continue;
      }
      const didDelete = await queueCleanupFile(fileName);
      if (didDelete) {
        deleted += 1;
      } else {
        failed.push({ file: fileName, reason: "locked_or_busy_retry_scheduled" });
      }
    }
    return res.json({ success: true, deleted, failed });
  } catch (error) {
    return res.status(500).json({
      success: false,
      message: error.message || "Cleanup failed"
    });
  }
});

// Error handler for multer
app.use((err, _req, res, _next) => {
  if (err instanceof multer.MulterError) {
    if (err.code === "LIMIT_FILE_SIZE") {
      return res.status(400).json({
        success: false,
        message: "File too large. Max size is 50MB."
      });
    }
    return res.status(400).json({
      success: false,
      message: err.message || "Upload failed"
    });
  }

  if (err) {
    console.error("Error:", err);
    return res.status(400).json({
      success: false,
      message: err.message || "Request failed"
    });
  }

  res.status(500).json({
    success: false,
    message: "Internal server error"
  });
});

// Conversion handler
async function processQueuedConversion({ inputPath, originalName, inputExt, outputExt, bitrate }, onProgress = () => {}) {
  if (isRouteTemporarilyBlocked(inputExt, outputExt)) {
    throw new Error(`Auto-protection active for ${makeRouteKey(inputExt, outputExt)} after repeated failures. Retry shortly.`);
  }
  const baseName = stripExt(originalName || "converted-file");
  const outputName = `${sanitizeName(baseName)}-${Date.now()}.${outputExt}`;
  const outputPath = path.join(downloadDir, outputName);
  try {
    let conversionResult;
    let lastErr = null;
    for (let attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt += 1) {
      try {
        onProgress(2, `Conversion attempt ${attempt}/${MAX_RETRY_ATTEMPTS}`);
        conversionResult = await runConversion({
          inputPath,
          outputPath,
          inputExt,
          outputExt,
          bitrate: String(bitrate || "").trim(),
          onProgress
        });
        lastErr = null;
        break;
      } catch (err) {
        lastErr = err;
        if (!isRetryableError(err) || attempt >= MAX_RETRY_ATTEMPTS) {
          break;
        }
        onProgress(10, `Transient issue detected. Retrying (${attempt}/${MAX_RETRY_ATTEMPTS})`);
        await sleep(RETRY_BASE_DELAY_MS * attempt);
      }
    }
    if (lastErr) {
      throw lastErr;
    }
    await ensureNonEmptyFile(outputPath);
    await safeDelete(inputPath);
    markSuccess(inputExt, outputExt);
    const response = {
      success: true,
      filename: outputName,
      url: `/download-once/${outputName}`
    };
    if (conversionResult?.extractedText) {
      response.extractedText = conversionResult.extractedText;
    }
    return response;
  } catch (error) {
    await safeDelete(inputPath);
    await safeDelete(outputPath);
    markFailure(inputExt, outputExt, error?.message || "conversion failed");
    throw error;
  }
}

async function runConversion({ inputPath, outputPath, inputExt, outputExt, bitrate, onProgress = () => {} }) {
  if (["jpg", "jpeg", "png", "webp"].includes(inputExt) && ["jpg", "png", "webp"].includes(outputExt)) {
    await sharp(inputPath)
      .toFormat(outputExt === "jpg" ? "jpeg" : outputExt)
      .toFile(outputPath);
    return;
  }

  if (OCR_IMAGE_EXTS.has(inputExt) && ["txt", "pdf", "docx"].includes(outputExt)) {
    onProgress(5, "OCR started");
    const extractedText = await ocrImageToText(inputPath, onProgress);
    onProgress(80, "OCR completed");
    await writeTextByFormat(extractedText, outputPath, outputExt);
    onProgress(100, "OCR output written");
    return { extractedText };
  }

  if (inputExt === "pdf" && ["txt", "pdf", "docx"].includes(outputExt)) {
    if (outputExt === "pdf") {
      // Keep PDF->PDF deterministic and avoid unnecessary OCR/re-encoding.
      await fs.copyFile(inputPath, outputPath);
      onProgress(100, "PDF copied");
      return;
    }

    onProgress(5, "Extracting PDF text");
    const directText = await tryExtractPdfText(inputPath);
    if (isLikelyUsefulPdfText(directText)) {
      onProgress(82, "Text extracted from PDF");
      await writeTextByFormat(directText, outputPath, outputExt);
      onProgress(100, "PDF text output written");
      return { extractedText: directText.trim() };
    }

    onProgress(5, "Preparing PDF for OCR");
    let renderedPath = "";
    try {
      renderedPath = await renderPdfFirstPageAsImage(inputPath);
    } catch (_renderErr) {
      // If no PDF rasterizer is available, still produce deterministic text output.
      const fallbackText = directText && directText.trim() ? directText : "No text detected";
      onProgress(82, "PDF rasterizer unavailable, using text fallback");
      await writeTextByFormat(fallbackText, outputPath, outputExt);
      onProgress(100, "PDF fallback output written");
      return { extractedText: fallbackText };
    }
    try {
      onProgress(20, "PDF render complete");
      const extractedText = await ocrImageToText(renderedPath, onProgress);
      onProgress(85, "OCR completed");
      await writeTextByFormat(extractedText, outputPath, outputExt);
      onProgress(100, "OCR output written");
      return { extractedText };
    } finally {
      await safeDelete(renderedPath);
    }
  }

  if (inputExt === "csv" && outputExt === "xlsx") {
    const workbook = XLSX.readFile(inputPath, { raw: true });
    XLSX.writeFile(workbook, outputPath);
    return;
  }

  if (inputExt === "xlsx" && outputExt === "csv") {
    const workbook = XLSX.readFile(inputPath, { raw: true });
    const firstSheet = workbook.SheetNames[0];
    if (!firstSheet) {
      throw new Error("No sheet found in XLSX file");
    }
    // FIX: Better empty sheet validation
    const data = XLSX.utils.sheet_to_csv(workbook.Sheets[firstSheet] || {});
    if (!data || data.trim().length === 0) {
      throw new Error("Excel sheet is empty or contains no data");
    }
    await fs.writeFile(outputPath, data, "utf8");
    return;
  }

  if (inputExt === "txt" && outputExt === "pdf") {
    await txtToPdf(inputPath, outputPath);
    return;
  }

  if (inputExt === "txt" && outputExt === "docx") {
    await txtToDocx(inputPath, outputPath);
    return;
  }

  if (inputExt === "docx" && outputExt === "txt") {
    const result = await mammoth.extractRawText({ path: inputPath });
    await fs.writeFile(outputPath, result.value || "", "utf8");
    return;
  }

  if (VIDEO_EXTS.has(inputExt) && ["mp3", "wav"].includes(outputExt)) {
    onProgress(5, "Audio extraction started");
    await extractAudioFromVideo(inputPath, outputPath, outputExt, bitrate, onProgress);
    onProgress(100, "Audio extraction completed");
    return;
  }

  if ((VIDEO_EXTS.has(inputExt) || AUDIO_EXTS.has(inputExt)) && ["txt", "srt", "pdf", "docx"].includes(outputExt)) {
    onProgress(5, "Extracting media text");
    let audioText = "";
    let audioChunks = [];
    try {
      const transcription = await speechToText(inputPath, inputExt, onProgress);
      audioText = (transcription.text || "").trim();
      audioChunks = transcription.chunks || [];
    } catch (speechErr) {
      if (AUDIO_EXTS.has(inputExt)) {
        throw speechErr;
      }
    }

    let visualText = "";
    if (VIDEO_EXTS.has(inputExt)) {
      visualText = await extractVisualTextFromVideo(inputPath, onProgress);
    }

    const mergedText = combineMediaTexts(audioText, visualText);
    const normalizedText = mergedText || "No text detected.";
    if (outputExt === "srt") {
      const chunks = audioChunks.length > 0 ? audioChunks : normalizeChunks([], normalizedText);
      await fs.writeFile(outputPath, toSrt(chunks), "utf8");
      onProgress(100, "Transcription srt written");
      return { extractedText: normalizedText };
    }
    if (outputExt === "txt") {
      await fs.writeFile(outputPath, normalizedText, "utf8");
      onProgress(100, "Transcription txt written");
      return { extractedText: normalizedText };
    }
    await writeTextByFormat(normalizedText, outputPath, outputExt);
    onProgress(100, "Text document written");
    return { extractedText: normalizedText };
  }

  throw new Error("No conversion handler configured for this format");
}

// Convert text to PDF
// FIX: Added proper error handling with try-catch
async function txtToPdf(inputPath, outputPath) {
  const text = await fs.readFile(inputPath, "utf8");

  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ margin: 42 });
      const stream = fsSync.createWriteStream(outputPath);

      stream.on("finish", resolve);
      stream.on("error", reject);
      doc.on("error", reject);

      doc.pipe(stream);
      doc.fontSize(11).text(text, {
        lineGap: 4,
        paragraphGap: 8
      });
      doc.end();
    } catch (error) {
      reject(error);
    }
  });
}

async function imagesToPdf(files, outputPath) {
  return new Promise(async (resolve, reject) => {
    const doc = new PDFDocument({ autoFirstPage: false, compress: true });
    const stream = fsSync.createWriteStream(outputPath);
    stream.on("finish", resolve);
    stream.on("error", reject);
    doc.on("error", reject);
    doc.pipe(stream);

    try {
      for (const file of files) {
        const inputBuffer = await fs.readFile(file.path);
        const prepared = await sharp(inputBuffer)
          .rotate()
          .flatten({ background: "#ffffff" })
          .jpeg({ quality: 92 })
          .toBuffer();
        const meta = await sharp(prepared).metadata();
        const width = Math.max(1, Math.round(meta.width || 1200));
        const height = Math.max(1, Math.round(meta.height || 1600));
        doc.addPage({ size: [width, height], margin: 0 });
        doc.image(prepared, 0, 0, { fit: [width, height], align: "center", valign: "center" });
      }
      doc.end();
    } catch (err) {
      reject(err);
    }
  });
}

// Convert text to DOCX
async function txtToDocx(inputPath, outputPath) {
  const text = await fs.readFile(inputPath, "utf8");
  const lines = text.split(/\r?\n/);
  const paragraphs = lines.map((line) => new Paragraph({ text: line || " " }));
  
  const doc = new Document({
    sections: [{ properties: {}, children: paragraphs }]
  });
  
  const buffer = await Packer.toBuffer(doc);
  await fs.writeFile(outputPath, buffer);
}

function normalizeBitrate(bitrate) {
  return bitrate === "320" || bitrate === "320k" || bitrate === "320kbps" ? "320k" : "128k";
}

function ensureFfmpegReady() {
  if (!resolvedFfmpegPath) {
    throw new Error(ffmpegProbeError || "FFmpeg is not available on this server");
  }
}

async function ocrImageToText(imagePath, onProgress = () => {}) {
  const googleVisionText = await tryGoogleVisionOcr(imagePath, onProgress);
  if (OCR_MODE === "cloud-first" && isLikelyUsefulOcrText(googleVisionText)) {
    onProgress(96, "Google Vision OCR selected");
    return googleVisionText.trim();
  }

  const paddleText = await tryPaddleOcr(imagePath, onProgress);
  if (isLikelyUsefulOcrText(paddleText)) {
    const winner = pickBestTexts([googleVisionText, paddleText]);
    if (winner === googleVisionText) {
      onProgress(95, "Google Vision OCR selected");
      return winner.trim();
    }
    onProgress(95, "PaddleOCR selected");
    return winner.trim();
  }

  const worker = await createWorker("eng");
  const quickCandidatePath = await buildQuickOcrCandidate(imagePath);
  const candidates = await buildOcrCandidates(imagePath);
  try {
    onProgress(20, "OCR engine ready");
    onProgress(28, "Running quick OCR");

    await worker.setParameters({
      tessedit_pageseg_mode: "6",
      preserve_interword_spaces: "1"
    });
    const quickResult = await worker.recognize(quickCandidatePath);
    const quickText = (quickResult?.data?.text || "").trim();
    const quickConfidence = Number(quickResult?.data?.confidence || 0);

    if (isStrongQuickOcrResult(quickText, quickConfidence)) {
      onProgress(95, "Quick OCR selected");
      return pickBestTexts([googleVisionText, paddleText, quickText]);
    }

    onProgress(35, "Prepared OCR variants");

    const psmModes = [6, 11, 3];
    const results = [];
    let step = 35;
    const totalRuns = candidates.length * psmModes.length;
    let runNo = 0;

    for (const candidatePath of candidates) {
      for (const psm of psmModes) {
        await worker.setParameters({
          tessedit_pageseg_mode: String(psm),
          preserve_interword_spaces: "1"
        });
        const result = await worker.recognize(candidatePath);
        const text = (result?.data?.text || "").trim();
        const confidence = Number(result?.data?.confidence || 0);
        results.push({ text, confidence, psm, candidatePath });
        runNo += 1;
        step = 35 + Math.round((runNo / totalRuns) * 55);
        onProgress(Math.min(step, 92), `OCR pass ${runNo}/${totalRuns}`);
      }
    }

    results.sort((a, b) => scoreOcrResult(b) - scoreOcrResult(a));
    const best = results[0];
    const tesseractText = best?.text || "";
    return pickBestTexts([googleVisionText, paddleText, tesseractText]);
  } finally {
    await safeDelete(quickCandidatePath);
    for (const candidatePath of candidates) {
      await safeDelete(candidatePath);
    }
    await worker.terminate();
  }
}

function pickBestTexts(texts) {
  const safe = (texts || []).filter((t) => typeof t === "string" && t.trim().length > 0);
  if (safe.length === 0) {
    return "";
  }
  safe.sort((a, b) => scoreOcrResult({ text: b, confidence: 70 }) - scoreOcrResult({ text: a, confidence: 70 }));
  return safe[0];
}

// FIX: Added timeout cleanup in catch block
async function tryGoogleVisionOcr(imagePath, onProgress = () => {}) {
  // Google Lens-like accuracy path. Requires:
  // 1) GOOGLE_VISION_API_KEY
  // 2) Internet access from server runtime
  const apiKey = String(process.env.GOOGLE_VISION_API_KEY || "").trim();
  if (!apiKey) {
    return "";
  }

  let timeout;
  try {
    onProgress(8, "Trying Google Vision OCR");
    const buffer = await fs.readFile(imagePath);
    const base64 = buffer.toString("base64");
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), 15000);

    const response = await fetch(
      `https://vision.googleapis.com/v1/images:annotate?key=${encodeURIComponent(apiKey)}`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          requests: [
            {
              image: { content: base64 },
              features: [{ type: "DOCUMENT_TEXT_DETECTION" }],
              imageContext: {
                languageHints: ["en", "hi"]
              }
            }
          ]
        }),
        signal: controller.signal
      }
    );
    clearTimeout(timeout);

    if (!response.ok) {
      return "";
    }
    const data = await response.json();
    const text =
      data?.responses?.[0]?.fullTextAnnotation?.text ||
      data?.responses?.[0]?.textAnnotations?.[0]?.description ||
      "";
    if (text && text.trim()) {
      onProgress(30, "Google Vision OCR completed");
      return text.trim();
    }
    return "";
  } catch (_err) {
    if (timeout) clearTimeout(timeout); // FIX: Clear timeout on error
    return "";
  }
}

// FIX: Improved PaddleOCR output parsing
async function tryPaddleOcr(imagePath, onProgress = () => {}) {
  // Optional handwritten-focused OCR. Enable with:
  // set PADDLE_OCR_ENABLED=true and install paddleocr CLI in environment.
  if (String(process.env.PADDLE_OCR_ENABLED || "").toLowerCase() !== "true") {
    return "";
  }

  onProgress(10, "Trying PaddleOCR");
  return new Promise((resolve) => {
    const args = [
      "--image_dir", imagePath,
      "--use_angle_cls", "true",
      "--use_gpu", "false"
    ];
    const proc = spawn("paddleocr", args, { windowsHide: true });
    let out = "";
    let err = "";

    proc.stdout.on("data", (chunk) => {
      out += chunk.toString();
    });
    proc.stderr.on("data", (chunk) => {
      err += chunk.toString();
    });
    proc.on("error", () => resolve(""));
    proc.on("close", (code) => {
      if (code !== 0) {
        return resolve("");
      }
      const extracted = parsePaddleOcrOutput(out || err);
      resolve(extracted);
    });
  });
}

// FIX: Better regex patterns for PaddleOCR parsing
function parsePaddleOcrOutput(raw) {
  if (!raw) {
    return "";
  }
  const lines = raw.split(/\r?\n/);
  const textParts = [];
  
  for (const line of lines) {
    // Try to match text in quotes with confidence score
    const match = line.match(/\[\[.*?\]\]\s*,\s*\[\s*\[\s*([\d.]+)\s*\]\s*\]/);
    if (match) {
      const textMatch = line.match(/'([^']+)'/);
      if (textMatch && textMatch[1]) {
        textParts.push(textMatch[1]);
      }
      continue;
    }
    
    // Try alternative pattern: text in single quotes
    const quoteMatch = line.match(/'([^']+)'\s*,\s*[\d.]+/);
    if (quoteMatch && quoteMatch[1]) {
      textParts.push(quoteMatch[1]);
      continue;
    }
    
    // Try to extract from INFO logs
    const infoMatch = line.match(/INFO:\s*(.+?)(?:\s*\d+\s*$|$)/);
    if (infoMatch && /[a-z]/i.test(infoMatch[1])) {
      const cleaned = infoMatch[1].trim();
      if (cleaned.length > 2 && !cleaned.includes("PaddleOCR")) {
        textParts.push(cleaned);
      }
    }
  }
  
  return textParts.join("\n").trim();
}

function isLikelyUsefulOcrText(text) {
  const cleaned = (text || "").replace(/\s+/g, "");
  if (cleaned.length < 12) {
    return false;
  }
  const alphaNumMatches = cleaned.match(/[a-z0-9]/gi) || [];
  return alphaNumMatches.length / cleaned.length >= 0.35;
}

function isLikelyUsefulPdfText(text) {
  const raw = String(text || "").trim();
  if (!raw) {
    return false;
  }
  if (isLikelyUsefulOcrText(raw)) {
    return true;
  }
  const alphaNumMatches = raw.match(/[a-z0-9]/gi) || [];
  return raw.length >= 6 && alphaNumMatches.length >= 4;
}

function isStrongQuickOcrResult(text, confidence) {
  if (!isLikelyUsefulOcrText(text)) {
    return false;
  }
  const words = (text.match(/[a-z0-9]{2,}/gi) || []).length;
  return confidence >= 45 && words >= 12;
}

function scoreOcrResult(result) {
  const text = result?.text || "";
  const confidence = Number(result?.confidence || 0);
  const cleaned = text.replace(/\s+/g, "");
  const words = (text.match(/[a-z0-9]{2,}/gi) || []).length;
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0).length;
  const alphaNum = (cleaned.match(/[a-z0-9]/gi) || []).length;
  const symbols = (cleaned.match(/[^a-z0-9]/gi) || []).length;
  const qualityRatio = cleaned.length ? alphaNum / cleaned.length : 0;
  const symbolPenalty = symbols > alphaNum ? 10 : 0;
  const shortPenalty = cleaned.length < 15 ? 15 : 0;
  const footerPenalty = /life is like riding a bicycle/i.test(text) ? 25 : 0;
  return (
    confidence +
    qualityRatio * 40 +
    Math.min(cleaned.length, 200) * 0.05 +
    Math.min(words, 80) * 0.6 +
    Math.min(lines, 20) * 0.4 -
    symbolPenalty -
    shortPenalty -
    footerPenalty
  );
}

async function buildOcrCandidates(inputPath) {
  const id = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
  const orientations = [0, 90];
  const metadata = await sharp(inputPath).metadata();
  const variants = [
    { name: "norm", threshold: null, sharpenSigma: 1.2 },
    { name: "bw165", threshold: 165, sharpenSigma: 1.2 }
  ];
  const cropModes = [
    { name: "full", marginRatio: 0 },
    { name: "inner", marginRatio: 0.08 }
  ];
  const outputPaths = [];

  for (const cropMode of cropModes) {
    for (const orientation of orientations) {
      for (const variant of variants) {
        try {
          const out = path.join(uploadDir, `ocr-${id}-${cropMode.name}-${orientation}-${variant.name}.png`);
          let pipeline = sharp(inputPath);

          if (cropMode.marginRatio > 0 && metadata.width && metadata.height) {
            const marginX = Math.floor(metadata.width * cropMode.marginRatio);
            const marginY = Math.floor(metadata.height * cropMode.marginRatio);
            const width = metadata.width - marginX * 2;
            const height = metadata.height - marginY * 2;
            if (width > 100 && height > 100) {
              pipeline = pipeline.extract({
                left: marginX,
                top: marginY,
                width,
                height
              });
            }
          }

          pipeline = pipeline
            .rotate()
            .rotate(orientation)
            .trim({ threshold: 8 })
            .grayscale()
            .normalize()
            .sharpen({ sigma: variant.sharpenSigma })
            .median(1)
            .resize({ width: 2600, fit: "inside", withoutEnlargement: true });

          if (variant.threshold !== null) {
            pipeline = pipeline.threshold(variant.threshold, { grayscale: true });
          }

          await pipeline.png({ compressionLevel: 9 }).toFile(out);
          outputPaths.push(out);
        } catch (_err) {
          // Ignore candidate generation failures and continue with remaining variants.
        }
      }
    }
  }

  if (outputPaths.length === 0) {
    const fallbackOut = path.join(uploadDir, `ocr-${id}-fallback.png`);
    await sharp(inputPath)
      .rotate()
      .grayscale()
      .normalize()
      .sharpen({ sigma: 1.2 })
      .resize({ width: 2200, fit: "inside", withoutEnlargement: true })
      .png({ compressionLevel: 9 })
      .toFile(fallbackOut);
    outputPaths.push(fallbackOut);
  }

  return outputPaths;
}

async function buildQuickOcrCandidate(inputPath) {
  const out = path.join(uploadDir, `ocr-quick-${Date.now()}-${Math.round(Math.random() * 1e9)}.png`);
  await sharp(inputPath)
    .rotate()
    .grayscale()
    .normalize()
    .sharpen({ sigma: 1.1 })
    .resize({ width: 2000, fit: "inside", withoutEnlargement: true })
    .png({ compressionLevel: 9 })
    .toFile(out);
  return out;
}

async function renderPdfFirstPageAsImage(inputPath) {
  const output = path.join(uploadDir, `ocr-pdf-${Date.now()}-${Math.round(Math.random() * 1e9)}.png`);
  try {
    await sharp(inputPath, { density: 300, page: 0 })
      .flatten({ background: "#ffffff" })
      .png({ compressionLevel: 9 })
      .toFile(output);
    return output;
  } catch (sharpErr) {
    // Fallback 1: pdftoppm (Poppler)
    const popplerOutBase = output.replace(/\.png$/, "");
    const popplerOut = `${popplerOutBase}.png`;
    try {
      await runExternalCommand("pdftoppm", ["-f", "1", "-singlefile", "-png", inputPath, popplerOutBase]);
      await ensureNonEmptyFile(popplerOut);
      return popplerOut;
    } catch (_popplerErr) {
      // Fallback 2: ImageMagick
      try {
        await runExternalCommand("magick", [
          "-density", "300",
          `${inputPath}[0]`,
          "-background", "white",
          "-alpha", "remove",
          "-alpha", "off",
          output
        ]);
        await ensureNonEmptyFile(output);
        return output;
      } catch (_magickErr) {
        throw new Error(
          `PDF OCR failed: no PDF rasterizer available (Sharp/Poppler/ImageMagick). Original error: ${sharpErr.message}`
        );
      }
    }
  }
}

async function tryExtractPdfText(inputPath) {
  try {
    const bytes = await fs.readFile(inputPath);
    const parsed = await pdfParse(bytes);
    return String(parsed?.text || "").trim();
  } catch (_err) {
    return "";
  }
}

async function runExternalCommand(command, args) {
  return new Promise((resolve, reject) => {
    let stderr = "";
    const proc = spawn(command, args, {
      windowsHide: true,
      stdio: ["ignore", "ignore", "pipe"]
    });

    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("error", (err) => {
      reject(new Error(`${command} failed to start: ${err.message}`));
    });

    proc.on("close", (code) => {
      if (code === 0) {
        return resolve();
      }
      const msg = (stderr || "").trim().slice(0, 600);
      reject(new Error(`${command} exited with code ${code}. ${msg}`));
    });
  });
}

async function writeTextByFormat(text, outputPath, outputExt) {
  const normalized = text && text.trim() ? text : "No text detected";
  if (outputExt === "txt") {
    await fs.writeFile(outputPath, normalized, "utf8");
    return;
  }
  if (outputExt === "pdf") {
    const tmpTxtPath = `${outputPath}.tmp.txt`;
    await fs.writeFile(tmpTxtPath, normalized, "utf8");
    try {
      await txtToPdf(tmpTxtPath, outputPath);
    } finally {
      await safeDelete(tmpTxtPath);
    }
    return;
  }
  if (outputExt === "docx") {
    const tmpTxtPath = `${outputPath}.tmp.txt`;
    await fs.writeFile(tmpTxtPath, normalized, "utf8");
    try {
      await txtToDocx(tmpTxtPath, outputPath);
    } finally {
      await safeDelete(tmpTxtPath);
    }
    return;
  }
  throw new Error("Unsupported text output format");
}

function normalizeTextLine(line) {
  return String(line || "")
    .replace(/\s+/g, " ")
    .trim();
}

function mergeTextBlocks(blocks) {
  const seen = new Set();
  const merged = [];
  for (const block of blocks) {
    const lines = String(block || "").split(/\r?\n+/);
    for (const line of lines) {
      const cleaned = normalizeTextLine(line);
      if (cleaned.length < 2) {
        continue;
      }
      const key = cleaned.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);
      merged.push(cleaned);
    }
  }
  return merged.join("\n").trim();
}

function combineMediaTexts(audioText, visualText) {
  const spoken = String(audioText || "").trim();
  const onScreen = String(visualText || "").trim();
  if (spoken && onScreen) {
    return `Audio Transcript:\n${spoken}\n\nOn-Screen Text:\n${onScreen}`;
  }
  return spoken || onScreen || "";
}

async function extractVisualTextFromVideo(inputPath, onProgress = () => {}) {
  ensureFfmpegReady();
  const id = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
  const frameDir = path.join(uploadDir, `video-frames-${id}`);
  await fs.mkdir(frameDir, { recursive: true });
  const framePattern = path.join(frameDir, "frame-%03d.jpg");
  const frameTexts = [];

  try {
    onProgress(12, "Sampling video frames");
    const args = [
      "-y",
      "-i", inputPath,
      "-vf", "fps=1/2,scale=1280:-2",
      "-q:v", "3",
      "-frames:v", "12",
      framePattern
    ];
    await runFfmpeg(args, onProgress);
    const frameFiles = (await fs.readdir(frameDir))
      .filter((name) => /^frame-\d+\.jpg$/i.test(name))
      .sort();

    for (let i = 0; i < frameFiles.length; i += 1) {
      const framePath = path.join(frameDir, frameFiles[i]);
      try {
        const text = await ocrImageToText(framePath, () => {});
        if (isLikelyUsefulOcrText(text)) {
          frameTexts.push(text);
        }
      } catch (_err) {
        // Ignore OCR failures for individual frames.
      }
      const pct = 20 + Math.round(((i + 1) / Math.max(frameFiles.length, 1)) * 35);
      onProgress(pct, `Reading video text ${i + 1}/${frameFiles.length}`);
    }

    return mergeTextBlocks(frameTexts);
  } finally {
    try {
      const files = await fs.readdir(frameDir);
      await Promise.all(files.map((name) => safeDelete(path.join(frameDir, name))));
      await fs.rmdir(frameDir).catch(() => {});
    } catch (_err) {
      // Ignore frame cleanup errors.
    }
  }
}

async function extractAudioFromVideo(inputPath, outputPath, outputExt, bitrate, onProgress = () => {}) {
  ensureFfmpegReady();
  const args = ["-y", "-i", inputPath, "-vn"];
  if (outputExt === "mp3") {
    args.push("-b:a", normalizeBitrate(bitrate), "-acodec", "libmp3lame", "-f", "mp3", outputPath);
  } else {
    args.push("-acodec", "pcm_s16le", "-ar", "44100", "-ac", "2", "-f", "wav", outputPath);
  }
  onProgress(25, "FFmpeg started");
  await runFfmpeg(args, onProgress);
  onProgress(90, "FFmpeg finished");
}

// FIX: Prevent race condition in Whisper pipeline loading
async function getWhisperPipeline() {
  if (whisperPipeline) {
    return whisperPipeline;
  }
  
  if (whisperLoadingPromise) {
    return whisperLoadingPromise;
  }
  
  whisperLoadingPromise = (async () => {
    try {
      const { pipeline } = await import("@xenova/transformers");
      whisperPipeline = await pipeline("automatic-speech-recognition", ASR_MODEL);
      return whisperPipeline;
    } finally {
      whisperLoadingPromise = null;
    }
  })();
  
  return whisperLoadingPromise;
}

async function speechToText(inputPath, inputExt, onProgress = () => {}) {
  if (!(VIDEO_EXTS.has(inputExt) || inputExt === "mp3" || inputExt === "wav")) {
    throw new Error("Unsupported media input for speech-to-text");
  }

  onProgress(15, "Preparing audio for ASR");
  const audio = await decodeAudioToFloat32(inputPath, onProgress);
  if (!audio || audio.length === 0) {
    throw new Error("Unable to decode audio for transcription");
  }

  onProgress(45, "Loading Whisper model");
  const asr = await getWhisperPipeline();
  onProgress(65, "Running transcription");
  let result = await asr(audio, {
    sampling_rate: 16000,
    chunk_length_s: 30,
    stride_length_s: 5,
    return_timestamps: true
  });
  let text = (result?.text || "").trim();

  // Retry with a simpler decode path when initial timestamped run is blank.
  if (!text) {
    onProgress(78, "Retrying transcription");
    result = await asr(audio, {
      sampling_rate: 16000,
      chunk_length_s: 20,
      stride_length_s: 3,
      return_timestamps: false
    });
    text = (result?.text || "").trim();
  }

  onProgress(90, "Transcription completed");
  const chunks = normalizeChunks(result?.chunks || [], text);
  return { text, chunks };
}

async function convertToWhisperWav(inputPath, outputPath, onProgress = () => {}) {
  ensureFfmpegReady();
  const args = [
    "-y",
    "-i", inputPath,
    "-vn",
    "-acodec", "pcm_s16le",
    "-ac", "1",
    "-ar", "16000",
    "-f", "wav",
    outputPath
  ];
  onProgress(22, "Converting media to WAV");
  await runFfmpeg(args, onProgress);
  onProgress(40, "WAV conversion done");
}

async function decodeAudioToFloat32(inputPath, onProgress = () => {}) {
  ensureFfmpegReady();
  return new Promise((resolve, reject) => {
    const bin = resolvedFfmpegPath || "ffmpeg";
    const args = [
      "-hide_banner",
      "-loglevel", "error",
      "-i", inputPath,
      "-vn",
      "-ac", "1",
      "-ar", "16000",
      "-f", "f32le",
      "pipe:1"
    ];
    const stdoutChunks = [];
    let stderr = "";

    try {
      const proc = spawn(bin, args, { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
      proc.stdout.on("data", (chunk) => {
        stdoutChunks.push(chunk);
        onProgress(35, "Decoding audio");
      });
      proc.stderr.on("data", (chunk) => {
        stderr += chunk.toString();
      });
      proc.on("error", (err) => {
        if (/EPERM/i.test(String(err?.message || ""))) {
          markSpawnEperm(`FFmpeg decode spawn error: ${err.message}`);
        }
        // If bundled ffmpeg path fails to spawn, retry once with system ffmpeg.
        if (bin !== "ffmpeg") {
          resolvedFfmpegPath = "ffmpeg";
          return decodeAudioToFloat32(inputPath, onProgress).then(resolve).catch(reject);
        }
        reject(new Error(`FFmpeg decode failed to start: ${err.message}`));
      });
      proc.on("close", (code) => {
        if (code !== 0) {
          const msg = (stderr || "").trim().slice(0, 800);
          return reject(new Error(`FFmpeg audio decode failed with code ${code}. ${msg}`));
        }
        const pcm = Buffer.concat(stdoutChunks);
        if (pcm.length < 4) {
          return resolve(new Float32Array());
        }
        const aligned = pcm.length - (pcm.length % 4);
        const trimmed = aligned === pcm.length ? pcm : pcm.subarray(0, aligned);
        const out = new Float32Array(trimmed.buffer, trimmed.byteOffset, trimmed.length / 4);
        return resolve(new Float32Array(out));
      });
    } catch (err) {
      reject(new Error(`FFmpeg audio decode failed: ${err.message}`));
    }
  });
}

async function runFfmpeg(args, onProgress = () => {}) {
  return new Promise((resolve, reject) => {
    const bin = resolvedFfmpegPath || "ffmpeg";
    let stderr = "";
    try {
      const proc = spawn(bin, args, { windowsHide: true });
      proc.stderr.on("data", (chunk) => {
        const text = chunk.toString();
        stderr += text;
        if (/time=\d/.test(text)) {
          onProgress(60, "FFmpeg processing");
        }
      });
      proc.on("error", (err) => {
        if (/EPERM/i.test(String(err?.message || ""))) {
          markSpawnEperm(`FFmpeg spawn error: ${err.message}`);
        }
        // If bundled ffmpeg path fails to spawn, retry once with system ffmpeg.
        if (bin !== "ffmpeg") {
          resolvedFfmpegPath = "ffmpeg";
          return runFfmpeg(args, onProgress).then(resolve).catch(reject);
        }
        reject(new Error(`FFmpeg spawn failed: ${err.message}`));
      });
      proc.on("close", (code) => {
        if (code === 0) {
          return resolve();
        }
        const msg = (stderr || "").trim().slice(0, 800);
        reject(new Error(`FFmpeg failed with code ${code}. ${msg}`));
      });
    } catch (err) {
      reject(new Error(`FFmpeg failed to start. ${err.message}`));
    }
  });
}

function makeProgressLogger(requestId) {
  let lastPercent = -1;
  return (percent, detail) => {
    const pct = Number.isFinite(percent) ? Math.max(0, Math.min(100, Math.round(percent))) : null;
    if (pct !== null && pct === lastPercent) {
      return;
    }
    if (pct !== null) {
      lastPercent = pct;
      console.log(`[convert:${requestId}] ${pct}% ${detail || ""}`.trim());
      return;
    }
    console.log(`[convert:${requestId}] ${detail || "progress update"}`);
  };
}

// FIX: Better duration calculation for chunks
function normalizeChunks(chunks, fallbackText) {
  if (chunks.length > 0) {
    return chunks.map((chunk, idx) => {
      const ts = chunk.timestamp || [idx * 4, (idx + 1) * 4];
      return {
        text: (chunk.text || "").trim() || "...",
        start: Number.isFinite(ts[0]) ? ts[0] : idx * 4,
        end: Number.isFinite(ts[1]) ? ts[1] : (idx + 1) * 4
      };
    });
  }
  
  // FIX: Better fallback duration calculation
  // Assume average speaking rate of 150 words per minute = 2.5 words per second
  const wordCount = (fallbackText || "").split(/\s+/).filter(w => w.length > 0).length;
  const estimatedDuration = Math.max(2, Math.ceil(wordCount / 2.5));
  
  return [{
    text: fallbackText || "No speech detected.",
    start: 0,
    end: estimatedDuration
  }];
}

function toSrt(chunks) {
  return chunks
    .map((chunk, idx) => {
      const start = formatSrtTime(chunk.start);
      const end = formatSrtTime(Math.max(chunk.end, chunk.start + 0.5));
      return `${idx + 1}\n${start} --> ${end}\n${chunk.text}\n`;
    })
    .join("\n");
}

function formatSrtTime(seconds) {
  const totalMs = Math.max(0, Math.floor(seconds * 1000));
  const ms = totalMs % 1000;
  const totalSec = Math.floor(totalMs / 1000);
  const sec = totalSec % 60;
  const totalMin = Math.floor(totalSec / 60);
  const min = totalMin % 60;
  const hr = Math.floor(totalMin / 60);
  return `${String(hr).padStart(2, "0")}:${String(min).padStart(2, "0")}:${String(sec).padStart(2, "0")},${String(ms).padStart(3, "0")}`;
}

// Cleanup old generated files (30 minutes)
async function cleanupOldFiles() {
  try {
    const now = Date.now();
    const maxAge = 30 * 60 * 1000; // 30 minutes
    const targets = [downloadDir, uploadDir];

    for (const dir of targets) {
      const files = await fs.readdir(dir);
      for (const file of files) {
        const filePath = path.join(dir, file);
        try {
          if (file.includes(".tmp.")) {
            await safeDelete(filePath);
            continue;
          }
          const stats = await fs.stat(filePath);
          if (stats.isFile() && now - stats.mtime.getTime() > maxAge) {
            await safeDelete(filePath);
            console.log(`Cleaned up old file: ${filePath}`);
          }
        } catch (_err) {
          // Skip files that can't be checked
        }
      }
    }
  } catch (error) {
    console.error("Error during cleanup:", error);
  }
}

// Initialize server
async function initializeServer() {
  try {
    await ensureDirs();
    const startupFiles = await fs.readdir(downloadDir).catch(() => []);
    for (const fileName of startupFiles) {
      if (isSafeGeneratedName(fileName)) {
        pendingCleanupFiles.add(fileName);
      }
    }
    if (pendingCleanupFiles.size) {
      scheduleCleanupRetry();
    }
    if (QUEUE_ENABLED) {
      const queue = getConversionQueue();
      await queue.waitUntilReady();
      console.log(`Queue ready: ${QUEUE_NAME} (${REDIS_URL})`);
    }
    
    // Run cleanup every 5 minutes (deletes files older than 30 minutes)
    setInterval(cleanupOldFiles, 5 * 60 * 1000);
    
    app.listen(PORT, () => {
      console.log(`FileForge backend running on http://localhost:${PORT}`);
      console.log(`Upload directory: ${uploadDir}`);
      console.log(`Download directory: ${downloadDir}`);
    });
  } catch (error) {
    console.error("Failed to initialize server:", error);
    process.exit(1);
  }
}

if (require.main === module) {
  initializeServer();
}

module.exports = {
  app,
  ensureDirs,
  processQueuedConversion,
  makeProgressLogger,
  markSpawnEperm,
  QUEUE_NAME,
  REDIS_URL
};
