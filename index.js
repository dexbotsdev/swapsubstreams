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

// const TOKEN = process.env.SUBSTREAMS_API_TOKEN
// const SPKG = "https://spkg.io/streamingfast/ethereum-explorer-v0.1.2.spkg"
// const MODULE = "map_block_meta"

const ENDPOINT = "https://mainnet.sol.streamingfast.io"

const START_BLOCK = '321185797'
const STOP_BLOCK = '+1'
const TOKEN  = 'eyJhbGciOiJLTVNFUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NzU3NjcxNzUsImp0aSI6IjRjNmE4ZmI5LTM0ZGEtNDczOS1hZmIxLTFlMzliNGE5Njg0OSIsImlhdCI6MTczOTc2NzE3NSwiaXNzIjoiZGZ1c2UuaW8iLCJzdWIiOiIwaHl0aTNjNTEwYmNiNTIzOTliNzEiLCJ2IjoxLCJha2kiOiJjYmZjYjUxZjM4YWQ2MDAxODZlN2U3NmQxYzEwYmVjOGM1NjUwNDExNzUwNzcxYjA4NTYyMzg4MDAwOTM1ZWVjIiwidWlkIjoiMGh5dGkzYzUxMGJjYjUyMzk5YjcxIn0.nbgU2hEph_494xFcmFvZV40VHJ7xmj_SP0mnC7OK-zXUkL3VteO9UODmIYVBVS4dmghShQ7N8SYQWBBfHkPCpw'
const SPKG = "https://spkg.io/v1/packages/tl_solana_dex_trades_1_0_22/v1.0.22";
const MODULE = "map_block";



/*
    Entrypoint of the application.
    Because of the long-running connection, Substreams will disconnect from time to time.
    The application MUST handle disconnections and commit the provided cursor to avoid missing information.
*/
const main = async () => {
    const pkg = await fetchPackage()
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
            await stream(pkg, registry, transport);

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

const fetchPackage = async () => {
    return await fetchSubstream(SPKG)
}

const stream = async (pkg, registry, transport) => {
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
        await handleResponseMessage(response.message, registry);

         //console.log(response)
    }
}

main()
