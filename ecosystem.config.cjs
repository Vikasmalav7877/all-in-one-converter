module.exports = {
  apps: [
    {
      name: "fileforge-converter",
      script: "server.js",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      restart_delay: 3000,
      kill_timeout: 5000,
      env: {
        NODE_ENV: "development",
        PORT: 3000
      },
      env_production: {
        NODE_ENV: "production",
        PORT: 3000,
        QUEUE_ENABLED: "true",
        REDIS_URL: "redis://127.0.0.1:6379",
        CONVERSION_QUEUE_NAME: "conversion-jobs"
      }
    },
    {
      name: "fileforge-worker",
      script: "worker.js",
      instances: 1,
      exec_mode: "fork",
      watch: false,
      autorestart: true,
      max_restarts: 10,
      min_uptime: "10s",
      restart_delay: 3000,
      kill_timeout: 5000,
      env: {
        NODE_ENV: "development",
        QUEUE_ENABLED: "true",
        REDIS_URL: "redis://127.0.0.1:6379",
        CONVERSION_QUEUE_NAME: "conversion-jobs",
        WORKER_CONCURRENCY: 10
      },
      env_production: {
        NODE_ENV: "production",
        QUEUE_ENABLED: "true",
        REDIS_URL: "redis://127.0.0.1:6379",
        CONVERSION_QUEUE_NAME: "conversion-jobs",
        WORKER_CONCURRENCY: 10
      }
    }
  ]
};
