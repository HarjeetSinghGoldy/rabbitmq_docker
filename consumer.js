// consumer.js
const amqp = require("amqplib");

// -------- ENV / CONFIG --------
const URL = process.env.RABBIT_URL || "amqp://myuser:mypass123!@localhost:5672/myvhost";
const MAIN_EX = process.env.MAIN_EX || "main.ex";
const MAIN_Q = process.env.MAIN_Q || "main.q";
const MAIN_RK = process.env.MAIN_RK || "main";
const RETRY_EX = process.env.RETRY_EX || "retry.ex";
const RETRY_Q = process.env.RETRY_Q || "retry.q";
const RETRY_RK = process.env.RETRY_RK || "retry";
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY_MS || "10000", 10); // 10s
const DLQ_EX = process.env.DLQ_EX || "dlq.ex";
const DLQ_Q = process.env.DLQ_Q || "dlq.q";
const DLQ_RK = process.env.DLQ_RK || "dlq";
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || "5", 10);

const QUEUE = MAIN_Q; // consume from main.q
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);
const BATCH_TIMEOUT = parseInt(process.env.BATCH_TIMEOUT || "2000", 10);
const SIMULATE_MS = parseInt(process.env.SIMULATE_MS || "60000", 10);

// -------- PRINT ENV VARS --------
console.log("==== Environment Variables ====");
console.log("RABBIT_URL =", process.env.RABBIT_URL ? process.env.RABBIT_URL.replace(/:\/\/([^:@]+):[^@]+@/, "://$1:****@") : "(default)");
console.log("MAIN_EX    =", process.env.MAIN_EX    || "(default)");
console.log("MAIN_Q     =", process.env.MAIN_Q     || "(default)");
console.log("MAIN_RK    =", process.env.MAIN_RK    || "(default)");
console.log("RETRY_EX   =", process.env.RETRY_EX   || "(default)");
console.log("RETRY_Q    =", process.env.RETRY_Q    || "(default)");
console.log("RETRY_RK   =", process.env.RETRY_RK   || "(default)");
console.log("RETRY_DELAY_MS =", process.env.RETRY_DELAY_MS || "(default)");
console.log("DLQ_EX     =", process.env.DLQ_EX     || "(default)");
console.log("DLQ_Q      =", process.env.DLQ_Q      || "(default)");
console.log("DLQ_RK     =", process.env.DLQ_RK     || "(default)");
console.log("MAX_RETRIES=", process.env.MAX_RETRIES|| "(default)");
console.log("BATCH_SIZE =", process.env.BATCH_SIZE || "(default)");
console.log("BATCH_TIMEOUT =", process.env.BATCH_TIMEOUT || "(default)");
console.log("SIMULATE_MS   =", process.env.SIMULATE_MS   || "(default)");
console.log("================================");

// -------- INTERNAL --------
let conn, ch;
let buf = [];
let timer = null;
let running = true;

// -------- TOPOLOGY SETUP --------
async function ensureTopology(ch) {
    await ch.assertExchange(MAIN_EX, "direct", { durable: true });
    await ch.assertExchange(RETRY_EX, "direct", { durable: true });
    await ch.assertExchange(DLQ_EX, "direct", { durable: true });

    await ch.assertQueue(MAIN_Q, {
        durable: true,
        arguments: {
            "x-dead-letter-exchange": RETRY_EX,
            "x-dead-letter-routing-key": RETRY_RK,
        },
    });

    await ch.bindQueue(MAIN_Q, MAIN_EX, MAIN_RK);

    await ch.assertQueue(RETRY_Q, {
        durable: true,
        arguments: {
            "x-message-ttl": RETRY_DELAY,
            "x-dead-letter-exchange": MAIN_EX,
            "x-dead-letter-routing-key": MAIN_RK,
        },
    });
    await ch.bindQueue(RETRY_Q, RETRY_EX, RETRY_RK);

    await ch.assertQueue(DLQ_Q, { durable: true });
    await ch.bindQueue(DLQ_Q, DLQ_EX, DLQ_RK);
}

// -------- STARTUP --------
( () => {
    conn = await amqp.connect(URL);
    conn.on("close", () => {
        console.error("[CONNECTION] closed – exiting for restart");
        process.exit(1);
    });
    conn.on("error", (e) => console.error("[CONNECTION] error:", e?.message || e));

    ch = await conn.createChannel();
    await ensureTopology(ch);

    await ch.prefetch(BATCH_SIZE);
    await ch.consume(QUEUE, onMsg, { noAck: false });

    console.log(`[READY] pid=${process.pid} consuming ${QUEUE} | batchSize=${BATCH_SIZE} timeout=${BATCH_TIMEOUT}ms`);
})().catch((e) => {
    console.error("startup error", e);
    process.exit(1);
});

// -------- CONSUME / BATCH --------
function onMsg(msg) {
    if (!running || !msg) return;
    buf.push(msg);

    if (buf.length >= BATCH_SIZE) {
        void flush();
        return;
    }
    if (!timer) timer = setTimeout(() => void flush(), BATCH_TIMEOUT);
}

async function flush() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (buf.length === 0) return;

    const batch = buf; buf = [];

    // Parse safely
    let payloads;
    try {
        payloads = batch.map((m) => JSON.parse(m.content.toString("utf8")));
    } catch (e) {
        console.error("[BATCH] JSON parse error, sending all to retry/DLQ");
        for (const m of batch) await handleFailure(m);
        return;
    }

    console.log(`[BATCH] pid=${process.pid} size=${batch.length} processing...`);
    try {
        // ---- Your real logic here ----
        // Decide if this batch is corrupt (look at first message's flag)
        const corruptBatch = payloads[0]?.corruptBatch;
        console.log(payloads,corruptBatch );

        if (corruptBatch) {
            throw new Error(`Corrupt batch flagged (batchIndex=${payloads[0].batchIndex})`);
        }
        await new Promise((r) => setTimeout(r, SIMULATE_MS));
        // --------------------------------
        batch.forEach((m) => ch.ack(m));
        console.log(`[BATCH] OK size=${batch.length}`);
    } catch (err) {
        console.error("[BATCH] FAILED, scheduling retries", err?.message || err);
        for (const m of batch) await handleFailure(m);
    }
}

// -------- RETRY / DLQ --------
function getRetryCount(msg) {
    const death = msg.properties?.headers?.["x-death"];
    if (!Array.isArray(death) || death.length === 0) return 0;
    // Count visits to main.q specifically
    const mainDeaths = death.filter((d) => d.queue === MAIN_Q);
    return mainDeaths.reduce((acc, d) => acc + (d.count || 1), 0);
}

async function handleFailure(msg) {
    const retries = getRetryCount(msg);
    if (retries >= MAX_RETRIES) {
        // Move to DLQ with original headers
        ch.publish(DLQ_EX, DLQ_RK, msg.content, {
            persistent: true,
            headers: msg.properties?.headers || {},
        });
        ch.ack(msg); // acknowledge original so it doesn’t loop forever
        console.warn(`[DLQ] moved message after ${retries} retries`);
    } else {
        // Dead-letter to retry.q (delay) by NACK (no requeue)
        ch.nack(msg, false, false);
        console.warn(`[RETRY] scheduled retry #${retries + 1}`);
    }
}

// -------- GRACEFUL SHUTDOWN --------
["SIGINT", "SIGTERM"].forEach((sig) =>
    process.on(sig, async () => {
        try {
            console.log(`[SHUTDOWN] ${sig} received, draining...`);
            running = false;
            await flush();
            if (ch) await ch.close().catch(() => { });
            if (conn) await conn.close().catch(() => { });
            console.log("[SHUTDOWN] closed cleanly");
        } finally {
            process.exit(0);
        }
    })
);

