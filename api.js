import express from 'express';
import pg from 'pg';
import cors from 'cors';
import { extractionQueue } from './queue.js';

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const pool = new pg.Pool({
  user: 'dipankar',
  host: 'localhost',
  database: 'analytics_db',
  port: 5432,
});

// 1. Producer Endpoint: Accepts single or batch documents and returns 202 immediately
app.post('/api/ingest', async (req, res) => {
  const start = Date.now();
  const { documents } = req.body; // Expects array of raw text strings

  if (!Array.isArray(documents) || documents.length === 0) {
    return res.status(400).json({ error: 'Provide a non-empty array of documents.' });
  }

  const jobs = await extractionQueue.addBulk(
    documents.map((text, idx) => ({
      name: 'extract-invoice',
      data: { rawText: text, batchIndex: idx },
    }))
  );

  return res.status(202).json({
    message: `Queued ${jobs.length} document(s) for async AI extraction.`,
    jobIds: jobs.map((j) => j.id),
    apiResponseMs: Date.now() - start,
  });
});

// 2. Observability Endpoint: Live Queue Depth + Warehouse Records
app.get('/api/telemetry', async (req, res) => {
  const counts = await extractionQueue.getJobCounts('waiting', 'active', 'completed', 'failed', 'delayed');
  const dbRows = await pool.query('SELECT * FROM extracted_invoices ORDER BY id DESC LIMIT 20');

  res.json({
    queueMetrics: counts,
    records: dbRows.rows,
  });
});

app.listen(4000, () => {
  console.log('🚀 Async API Gateway running on http://127.0.0.1:4000');
});
