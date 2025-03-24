import express from 'express';
import WebSocket, { WebSocketServer } from 'ws';
import dotenv from 'dotenv';
import cors from 'cors';
import http from 'http';
import axios from 'axios';
import {
    createRequest,
    isEmptyMessage,
    streamBlocks,
    unpackMapOutput,
    createAuthInterceptor,
    createRegistry,
    fetchSubstream,
  } from "@substreams/core";
  import { createConnectTransport } from "@bufbuild/connect-web";


import { getCursor } from "./cursor.js";
import { isErrorRetryable } from "./error.js";
import { handleResponseMessage, handleProgressMessage } from "./handlers.js"
 

const ENDPOINT = "https://mainnet.sol.streamingfast.io"

const START_BLOCK = '321185797'
const STOP_BLOCK = '+1'
const TOKEN  = 'eyJhbGciOiJLTVNFUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzU3NjcxNzUsImp0aSI6IjRjNmE4ZmI5LTM0ZGEtNDczOS1hZmIxLTFlMzliNGE5Njg0OSIsImlhdCI6MTczOTc2NzE3NSwiaXNzIjoiZGZ1c2UuaW8iLCJzdWIiOiIwaHl0aTNjNTEwYmNiNTIzOTliNzEiLCJ2IjoxLCJha2kiOiJjYmZjYjUxZjM4YWQ2MDAxODZlN2U3NmQxYzEwYmVjOGM1NjUwNDExNzUwNzcxYjA4NTYyMzg4MDAwOTM1ZWVjIiwidWlkIjoiMGh5dGkzYzUxMGJjYjUyMzk5YjcxIn0.nbgU2hEph_494xFcmFvZV40VHJ7xmj_SP0mnC7OK-zXUkL3VteO9UODmIYVBVS4dmghShQ7N8SYQWBBfHkPCpw'
const SPKG = "https://spkg.io/v1/packages/tl_solana_dex_trades_1_0_22/v1.0.22";
const MODULE = "map_block";



dotenv.config();

class AuthStreamService {
    static API_KEYS = (process.env.API_KEYS || '').split(',');
    static API_KEYS_URL = process.env.API_KEYS_URL || '';
    static PORT = process.env.PORT || 3000;
    static clientConn;

    constructor() {
        this.app = express();
        this.wss = new WebSocketServer({ noServer: true });
        this.server = http.createServer(this.app);
        this.clientConn={ send(data){console.log(data)}};
        this.setupMiddleware();
        this.setupWebSocket();
        AuthStreamService.fetchApiKeys();
        setInterval(() => AuthStreamService.fetchApiKeys(), 10 * 60 * 1000);
    }

    fetchPackage = async () => {
        return await fetchSubstream(SPKG)
    }

    async startStreaming() {
        const pkg = await this.fetchPackage()
            const registry = createRegistry(pkg);
        
            const transport = createConnectTransport({
                baseUrl: ENDPOINT,
                interceptors: [createAuthInterceptor(TOKEN)],
                useBinaryFormat: true,
                jsonOptions: {
                    typeRegistry: registry,
                },
            });
            
            // The infite loop handles disconnections. Every time an disconnection error is thrown, the loop will automatically reconnect
            // and start consuming from the latest commited cursor.
            while (true) {
                try {
                    await this.stream(pkg, registry, transport);
        
                    // Break out of the loop when the stream is finished
                    break;
                } catch (e) {
                    if (!isErrorRetryable(e)) {
                      console.log(`A fatal error occurred: ${e}`)
                      throw e
                    }
                    console.log(`A retryable error occurred (${e}), retrying after backoff`)
                    console.log(e)
                    // Add backoff from a an easy to use library
                }
            }
    }

    stream = async (pkg, registry, transport) => {
        const request = createRequest({
            substreamPackage: pkg,
            outputModule: MODULE,
            productionMode: true,
            startBlockNum: START_BLOCK, 
            startCursor: await getCursor() ?? undefined
        });
        
        // Stream the blocks
        for await (const response of streamBlocks(transport, request)) {
            /*https://spkg.io/v1/packages/tl_solana_dex_trades_1_0_22/v1.0.22
                Decode the response and handle the message.
                There different types of response messages that you can receive. You can read more about the response message in the docs:
                https://substreams.streamingfast.io/documentation/consume/reliability-guarantees#the-response-format
            */
            await handleResponseMessage(response.message, registry,this.clientConn);
    
             //console.log(response)
        }
    }

    setupMiddleware() {
        this.app.use(cors());
        this.app.use(express.json());
    }

    static async fetchApiKeys() {
        if (!AuthStreamService.API_KEYS_URL || AuthStreamService.API_KEYS_URL.includes('localhost')) return;
        try {
            const response = await axios.get(AuthStreamService.API_KEYS_URL);
            if (Array.isArray(response.data)) {
                AuthStreamService.API_KEYS = response.data;
                console.log('API keys updated successfully');
            }
        } catch (error) {
            console.error('Failed to fetch API keys:', error.message);
        }
    }

    setupWebSocket() {
        this.wss.on('connection', (ws, req) => {
            console.log('New client connected');
 
            this.clientConn=ws;
            ws.on('close', () => {
               // clearInterval(interval);
                console.log('Client disconnected');
            });

            ws.on('error', (err) => {
                console.error('WebSocket error:', err);
                ws.terminate();
            });
        });

        this.server.on('upgrade', (req, socket, head) => {
            const url = new URL(req.url || '', `http://${req.headers.host}`);
            if (url.hostname === 'localhost') {
                this.wss.handleUpgrade(req, socket, head, (ws) => {
                    this.wss.emit('connection', ws, req);
                });
                return;
            }
            const apiKey = url.searchParams.get('apiKey');
            // if (!AuthStreamService.API_KEYS.includes(apiKey) && url.hostname !== 'localhost') {
            //     socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
            //     socket.destroy();
            //     return;
            // }
            this.wss.handleUpgrade(req, socket, head, (ws) => {
                this.wss.emit('connection', ws, req);
            });
        });
    }
 

    start() {
        this.server.listen(AuthStreamService.PORT, () => {
            console.log(`Server running on port ${AuthStreamService.PORT}`);
            this.startStreaming()
        });
    }
}

const service = new AuthStreamService();
service.start();

// Auto-restart on failure
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});
