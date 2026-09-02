const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const crypto = require('crypto');
const Bottleneck = require('bottleneck');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static('public'));

const PORT = process.env.PORT || 3000;
const API_BASE = process.env.NESTLINK_API_BASE || 'https://automate.nestlink.co.ke/api';
const CLIENT_ID = process.env.NESTLINK_CLIENT_ID;
const CLIENT_SECRET = process.env.NESTLINK_CLIENT_SECRET;

// 9 requests per minute rate-limiter (60000ms / 9 = 6667ms interval)
const limiter = new Bottleneck({
  maxConcurrent: 1,
  minTime: 6667
});

function generateSignature(timestamp, nonce, idempotencyKey, body) {
  const payloadStr = `${timestamp}.${nonce}.${idempotencyKey}.${JSON.stringify(body)}`;
  return crypto.createHmac('sha256', CLIENT_SECRET).update(payloadStr).digest('hex');
}

async function sendStkPush(accountNumber, amount, phone) {
  const endpoint = '/v1/stkpush/initiate';
  const timestamp = Date.now().toString();
  const nonce = crypto.randomBytes(16).toString('hex');
  const idempotencyKey = crypto.randomUUID();

  const body = { account_number: accountNumber, amount: Number(amount) };
  if (phone) body.phone = phone;

  const signature = generateSignature(timestamp, nonce, idempotencyKey, body);

  const response = await axios.post(`${API_BASE}${endpoint}`, body, {
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': CLIENT_ID,
      'X-Timestamp': timestamp,
      'X-Nonce': nonce,
      'X-Idempotency-Key': idempotencyKey,
      'X-Signature': signature
    },
    timeout: 10000
  });

  return response.data;
}

const wrappedStkPush = limiter.wrap(sendStkPush);

io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  socket.on('start-bulk', async (payload) => {
    const { accountNumber, amount, phones } = payload;

    if (!phones || !Array.isArray(phones) || phones.length === 0) {
      return socket.emit('log', { type: 'error', message: 'Invalid phone list provided.' });
    }

    socket.emit('log', { 
      type: 'info', 
      message: `Queue started for ${phones.length} numbers (Rate limit: 9 requests/min).` 
    });

    for (let i = 0; i < phones.length; i++) {
      const phone = phones[i].trim();
      if (!phone) continue;

      socket.emit('log', {
        type: 'info',
        message: `[${i + 1}/${phones.length}] Dispatching STK push to ${phone}...`
      });

      try {
        const result = await wrappedStkPush(accountNumber, amount, phone);
        socket.emit('log', {
          type: 'success',
          message: `[SUCCESS] ${phone}: ${result.message || 'STK Push sent successfully'}`
        });
      } catch (err) {
        const errorMsg = err.response?.data?.message || err.message;
        socket.emit('log', {
          type: 'error',
          message: `[FAILED] ${phone}: ${errorMsg}`
        });
      }
    }

    socket.emit('log', { type: 'done', message: 'Bulk processing completed.' });
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
