import { Worker } from 'bullmq';
import pg from 'pg';
import { z } from 'zod';
import { redisConnection } from './queue.js';

const pool = new pg.Pool({
  user: 'dipankar',
  host: 'localhost',
  database: 'analytics_db',
  port: 5432,
});

// Strict Zod Schema Guardrail: Rejects hallucinated types or missing fields
const InvoiceSchema = z.object({
  vendor_name: z.string().min(1),
  invoice_number: z.string().nullable().optional(),
  total_amount: z.number().nonnegative(),
  currency: z.string().length(3),
  risk_flag: z.boolean(),
});

const worker = new Worker(
  'ai-extraction-queue',
  async (job) => {
    const startTime = Date.now();
    const { rawText } = job.data;

    console.log(`[Worker] Processing Job #${job.id} (Attempt ${job.attemptsMade + 1})...`);

    const prompt = `Extract structured invoice data from the raw text below.
Return ONLY a valid JSON object matching this exact schema:
{
  "vendor_name": "string",
  "invoice_number": "string or null",
  "total_amount": number,
  "currency": "3-letter ISO code like USD, INR, EUR",
  "risk_flag": boolean (true if total_amount > 10000 or text mentions overdue/urgent penalty)
}

Raw Document Text:
"""
${rawText}
"""`;

    const ollamaRes = await fetch('http://localhost:11434/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'qwen2.5-coder:7b',
        prompt,
        format: 'json', // Enforces structured JSON output at token sampling level
        stream: false,
        options: { temperature: 0 },
      }),
    });

    const ollamaData = await ollamaRes.json();

    // Parse & Validate through Zod Guardrail (throws error -> triggers BullMQ retry if invalid)
    const parsedJson = JSON.parse(ollamaData.response);
    const validated = InvoiceSchema.parse(parsedJson);

    const processingMs = Date.now() - startTime;

    // Persist validated record into PostgreSQL warehouse
    await pool.query(
      `INSERT INTO extracted_invoices (job_id, vendor_name, invoice_number, total_amount, currency, risk_flag, processing_ms)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (job_id) DO NOTHING`,
      [
        String(job.id),
        validated.vendor_name,
        validated.invoice_number || 'N/A',
        validated.total_amount,
        validated.currency.toUpperCase(),
        validated.risk_flag,
        processingMs,
      ]
    );

    console.log(`[Worker] ✅ Completed Job #${job.id} in ${processingMs}ms -> ${validated.vendor_name} (${validated.currency} ${validated.total_amount})`);
    return { ...validated, processingMs };
  },
  {
    connection: redisConnection,
    concurrency: 2, // Protects 16GB Unified Memory by processing max 2 GPU jobs simultaneously
  }
);

worker.on('failed', (job, err) => {
  console.error(`[Worker] ❌ Job #${job?.id} failed attempt ${job?.attemptsMade}: ${err.message}`);
});

console.log('⚙️  Async AI Worker listening for jobs (Concurrency: 2)...');
