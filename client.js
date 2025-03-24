import WebSocket from 'ws';

const API_KEY = 'YOUR_API_KEY';
const SERVER_URL = `ws://localhost:3000?apiKey=${API_KEY}`;

const ws = new WebSocket(SERVER_URL);

ws.on('open', () => {
    console.log('Connected to WebSocket server');
});

ws.on('message', (data) => {
    console.log('Received data:', JSON.parse(data));
});

ws.on('close', () => {
    console.log('Connection closed');
});

ws.on('error', (error) => {
    console.error('WebSocket error:', error);
});
