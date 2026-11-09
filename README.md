# High-Throughput Async AI Data Ingestion Pipeline

An enterprise-grade asynchronous AI extraction system built with **Node.js**, **Redis (BullMQ)**, **Ollama (Qwen2.5-Coder 7B)**, **Zod** schema guardrails, and **PostgreSQL**. Decouples sub-10ms HTTP ingestion (`202 Accepted`) from concurrent local GPU inference with automatic retries and live queue observability.

## System Architecture

```text
[Client / Batch Upload UI]
          │  POST /api/ingest
          ▼
[Express API Producer] ──► Returns HTTP 202 Accepted (< 10 ms)
          │
          ▼
[Redis Queue (BullMQ)] ──► Buffers bursts + Exponential Backoff (3 Retries)
          │
          ▼
[Concurrent GPU Workers (Concurrency: 2)]
          │  1. Local Open-Weight LLM (Qwen2.5-Coder 7B JSON Mode)
          │  2. Strict Runtime Validation (Zod Schema Guardrail)
          ▼
[PostgreSQL Warehouse] ──► Stores validated records + risk flags
```

## Key Engineering Highlights
* **Non-Blocking Ingestion (`202 Accepted`):** Decouples fast web requests from multi-second GPU inference using Redis-backed message queues (`BullMQ`).
* **Hardware-Aware Backpressure:** Caps worker concurrency at `2` to prevent Unified Memory saturation on 16 GB Apple Silicon while buffering burst traffic safely in Redis.
* **Deterministic JSON Guardrails:** Combines Ollama's token-level `format: "json"` with runtime **Zod** type validation and automatic exponential-backoff retries.

## Run Locally
1. Start services: `brew services start ollama postgresql@16 redis`
2. Start worker: `node worker.js`
3. Start API & UI: `node api.js` → Open `http://127.0.0.1:4000`
