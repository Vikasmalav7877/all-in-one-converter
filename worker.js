require("dotenv").config();
const IORedis = require("ioredis");
const { Worker } = require("bullmq");
const {
  ensureDirs,
  processQueuedConversion,
  makeProgressLogger,
  markSpawnEperm,
  QUEUE_NAME,
  REDIS_URL
} = require("./server");

const WORKER_CONCURRENCY = Math.max(1, Number.parseInt(process.env.WORKER_CONCURRENCY || "10", 10) || 10);

const connection = new IORedis(REDIS_URL, {
  maxRetriesPerRequest: null,
  enableReadyCheck: true
});

async function startWorker() {
  await ensureDirs();
  const worker = new Worker(
    QUEUE_NAME,
    async (job) => {
      const onProgress = (percent, detail) => {
        const pct = Number.isFinite(percent) ? Math.max(0, Math.min(100, Math.round(percent))) : 0;
        job.updateProgress({ percent: pct, detail: detail || "" }).catch(() => {});
      };
      const localProgress = makeProgressLogger(String(job.id || "job"));
      const combinedProgress = (percent, detail) => {
        localProgress(percent, detail);
        onProgress(percent, detail);
      };
      return processQueuedConversion(job.data, combinedProgress);
    },
    {
      connection,
      concurrency: WORKER_CONCURRENCY
    }
  );

  worker.on("ready", () => {
    console.log(`Worker ready. Queue=${QUEUE_NAME}, concurrency=${WORKER_CONCURRENCY}`);
  });

  worker.on("failed", (job, err) => {
    if (/spawn EPERM/i.test(String(err?.message || ""))) {
      markSpawnEperm(err.message);
    }
    console.error(`Job failed: ${job?.id || "unknown"} ${err?.message || ""}`.trim());
  });

  worker.on("error", (err) => {
    console.error("Worker error:", err);
  });
}

startWorker().catch((error) => {
  console.error("Failed to start worker:", error);
  process.exit(1);
});
