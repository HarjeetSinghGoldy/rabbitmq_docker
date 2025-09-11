import amqp from "amqplib";

const RABBIT_URL = process.env.RABBIT_URL || "amqp://admin:qwerty@rabbitmq-headless.rabbitmq.svc.cluster.local:5672";
const QUEUE = "batch_queue";

// Batch settings
const BATCH_SIZE = 100;
const BATCH_TIMEOUT = 2000; // ms – flush batch if not full

let channel;
let currentBatch = [];
let batchTimer = null;

 function start() {
  const conn = await amqp.connect(RABBIT_URL);
  channel = await conn.createChannel();
  await channel.assertQueue(QUEUE, { durable: true });

  // Only give this consumer up to 100 unacked messages at a time
  channel.prefetch(BATCH_SIZE);

  console.log(" [*] Waiting for messages in", QUEUE);

  channel.consume(QUEUE, onMessage, { noAck: false });
}

function onMessage(msg) {
  if (!msg) return;

  currentBatch.push(msg);

  // If batch reached target size → process immediately
  if (currentBatch.length >= BATCH_SIZE) {
    flushBatch();
  }

  // Otherwise set/refresh a timer to flush partial batch
  if (!batchTimer) {
    batchTimer = setTimeout(() => {
      flushBatch();
    }, BATCH_TIMEOUT);
  }
}

async function flushBatch() {
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  if (currentBatch.length === 0) return;

  const msgs = currentBatch;
  currentBatch = [];

  try {
    // Convert to payloads
    const payloads = msgs.map(m => JSON.parse(m.content.toString()));

    console.log(` [x] Processing batch of ${payloads.length} messages`);

    // TODO: Replace with your real business logic
    await fakeProcess(payloads);

    // Acknowledge all messages in batch
    msgs.forEach(m => channel.ack(m));
  } catch (err) {
    console.error("Batch failed", err);
    // Reject messages so they can be retried or sent to DLX
    msgs.forEach(m => channel.nack(m, false, false));
  }
}

async function fakeProcess(batch) {
  return new Promise(res => setTimeout(res, 500));
}

start().catch(err => {
  console.error("Error starting consumer", err);
  process.exit(1);
});

