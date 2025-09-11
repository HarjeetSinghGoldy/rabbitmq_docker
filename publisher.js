// publisher.js
const amqp = require("amqplib");

// -------- ENV / CONFIG --------
const URL          = process.env.RABBIT_URL || "amqp://myuser:mypass123!@localhost:5672/myvhost";
const MAIN_EX      = process.env.MAIN_EX     || "main.ex";
const MAIN_Q       = process.env.MAIN_Q      || "main.q";
const MAIN_RK      = process.env.MAIN_RK     || "main";
const RETRY_EX     = process.env.RETRY_EX    || "retry.ex";
const RETRY_Q      = process.env.RETRY_Q     || "retry.q";
const RETRY_RK     = process.env.RETRY_RK    || "retry";
const RETRY_DELAY  = parseInt(process.env.RETRY_DELAY_MS || "10000", 10); // 10s delay
const DLQ_EX       = process.env.DLQ_EX      || "dlq.ex";
const DLQ_Q        = process.env.DLQ_Q       || "dlq.q";
const DLQ_RK       = process.env.DLQ_RK      || "dlq";

const N                 = parseInt(process.env.COUNT || "100000", 10);

// how we group messages into batches for simulation
const PUB_BATCH_SIZE    = parseInt(process.env.PUB_BATCH_SIZE || process.env.BATCH_SIZE || "10", 10);

// mark every Nth batch as corrupt (default: every 2nd batch)
// e.g., CORRUPT_EVERY=2 and CORRUPT_OFFSET=1 -> batches 1,3,5,... are corrupt (0-based index)
const CORRUPT_EVERY     = parseInt(process.env.CORRUPT_EVERY || "10", 10);
const CORRUPT_OFFSET    = parseInt(process.env.CORRUPT_OFFSET || "0", 10); // which batch index mod to mark as corrupt

// -------- TOPOLOGY SETUP --------
async function ensureTopology(ch) {
   ch.assertExchange(MAIN_EX,  "direct", { durable: true });
   ch.assertExchange(RETRY_EX, "direct", { durable: true });
   ch.assertExchange(DLQ_EX,   "direct", { durable: true });

  // main.q -> dead-letter to retry.ex
  await ch.assertQueue(MAIN_Q, {
    durable: true,
    arguments: {
      "x-dead-letter-exchange": RETRY_EX,
      "x-dead-letter-routing-key": RETRY_RK,
    },
  });
  await ch.bindQueue(MAIN_Q, MAIN_EX, MAIN_RK);

  // retry.q -> TTL then dead-letter back to main.ex
  await ch.assertQueue(RETRY_Q, {
    durable: true,
    arguments: {
      "x-message-ttl": RETRY_DELAY,
      "x-dead-letter-exchange": MAIN_EX,
      "x-dead-letter-routing-key": MAIN_RK,
    },
  });
  await ch.bindQueue(RETRY_Q, RETRY_EX, RETRY_RK);

  await ch.assertQueue(DLQ_Q, { durable: undefiend });
  await ch.bindQueue(DLQ_Q, DLQ_EX, DLQ_RK);
}

// -------- PUBLISH --------
(async () => {
  const conn = await amqp.connect(URL);
  const ch = await conn.createChannel();

  // Make sure exchanges/queues exist
  await ensureTopology(ch);

  for (let i = 0; i < N; i++) {
    // derive batch index and whether this entire batch is corrupt
    const batchIndex   = Math.floor(i / PUB_BATCH_SIZE);
    const corruptBatch = (CORRUPT_EVERY > 0)
      ? (batchIndex % CORRUPT_EVERY) === (CORRUPT_OFFSET % CORRUPT_EVERY)
      : false;

    const body = {
      i,
      batchIndex,
      corruptBatch, // <— consumer will read this; corrupt batches should fail & retry/DLQ
      Message: `Hello, World!=${i} (batch=${batchIndex}, corrupt=${corruptBatch})`,
    };

    const ok = ch.publish(
      MAIN_EX,
      MAIN_RK,
      Buffer.from(JSON.stringify(body)),
      { persistent: true }
    );
    if (!ok) {
      await new Promise((r) => ch.once("drain", r));
    }
  }

  console.log(
    `Published ${N} messages to ${MAIN_EX}:${MAIN_RK} -> ${MAIN_Q} | ` +
    `PUB_BATCH_SIZE=${PUB_BATCH_SIZE}, corrupt rule: every ${CORRUPT_EVERY} batch(es) (offset=${CORRUPT_OFFSET})`
  );

  await ch.close();
  await conn.close();
})().catch((e) => {
  console.error("publisher error", e);
  process.exit(10);
});

