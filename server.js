import express from 'express';
import WebSocket, { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import cors from 'cors';
import http from 'http';
import axios from 'axios';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;
let API_KEYS = (process.env.API_KEYS || '').split(',');
const API_KEYS_URL = process.env.API_KEYS_URL || '';

app.use(cors());
app.use(express.json());

// Function to fetch API keys from an external URL
const fetchApiKeys = async () => {
    if (!API_KEYS_URL || API_KEYS_URL.includes('localhost')) return;
    if (!API_KEYS_URL) return;
    try {
        const response = await axios.get(API_KEYS_URL);
        if (Array.isArray(response.data)) {
            API_KEYS = response.data;
            console.log('API keys updated successfully');
        }
    } catch (error) {
        console.error('Failed to fetch API keys:', error.message);
    }
};

// Fetch API keys every 10 minutes
setInterval(fetchApiKeys, 10 * 60 * 1000);
fetchApiKeys();

// WebSocket Server Setup
const wss = new WebSocketServer({ noServer: true });

wss.on('connection', (ws, req) => {
    console.log('New client connected');
    
    // Simulated data streaming
    const sendData = () => {
        const sampleData = {
            blockDate: "2025-02-17",
            blockTime: "1739772085",
            blockSlot: "321193035",
            txId: "2n1WuGn97fnUK8hwjdWfRsrzGq2k3qCh2HgTgZTpRXtsNirtk1jRWwbTrYBpVNTncU3iMKhmdY7FHCcadBkpmsX9",
            signer: "GXDaLFNw1aYF8FTfqzXMqWVrvMPGiVpYhcfBL2bLG2fD",
            poolAddress: "SzYghMeXeaLrcVZyi27563JYgpiBWzq1TytGDGxLKv1",
            baseMint: "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
            quoteMint: "So11111111111111111111111111111111111111112",
            baseAmount: 99.9,
            quoteAmount: -0.454496494,
            instructionType: "Swap"
        };
        ws.send(JSON.stringify(sampleData));
    };

    const interval = setInterval(sendData, 2000);

    ws.on('close', () => {
        clearInterval(interval);
        console.log('Client disconnected');
    });
    
    ws.on('error', (err) => {
        console.error('WebSocket error:', err);
        ws.terminate();
    });
});

// HTTP Upgrade for WebSocket Authentication
const server = http.createServer(app);
server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

server.on('upgrade', (req, socket, head) => {
    const url = new URL(req.url || '', `http://${req.headers.host}`);
    const apiKey = url.searchParams.get('apiKey');
    
    if (!API_KEYS.includes(apiKey) && url.hostname !== 'localhost') {
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req);
    });
});

// Auto-restart on failure
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});
