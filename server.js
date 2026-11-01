import { WebSocketServer, WebSocket } from "ws";
import crypto from 'crypto';
import { parse } from "url";
import http from 'http';
//import { Redis } from '@upstash/redis';
import Redis from 'ioredis';
import { db } from './db/offlineMessage.js';
import { authenticateToken } from './db/auth.js'; 
const localConnections = new Map();
const SERVER_ID = `server_${crypto.randomBytes(4).toString('hex')}`;
const PORT = process.env.PORT || 8080;

//const wss = new WebSocketServer({ port: PORT });
//console.log(`Chat server [${SERVER_ID}] running on port ${PORT}`);
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200);
    return res.end('ok');
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Chat server [${SERVER_ID}] running on port ${PORT}`);
});

// redis configuration for pub and sub
const redisUrl = process.env.REDIS_URL;
const pub = new Redis(redisUrl);
const sub = new Redis(redisUrl);

const CHANNEL_NAME = `channel_${SERVER_ID}`;
const REGISTRY_KEY = 'presence_registry';

sub.subscribe(CHANNEL_NAME, (err) => {
    if (err) {
        console.error("Error subscribing to Redis channel:", err);
    }
});

sub.on('message', (channel, incomingPayload) => {
    if (channel !== CHANNEL_NAME) return;

    try {
        const { targetId } = JSON.parse(incomingPayload);

        if (!targetId) return;

        // If the targeted user lives on this specific server instance, deliver it
        const localTargetWs = localConnections.get(targetId);
        if (localTargetWs && localTargetWs.readyState === WebSocket.OPEN) {
            // The published payload is already the serialized structuredPayload
            localTargetWs.send(incomingPayload);
        }
    } catch (err) {
        console.error(`[${SERVER_ID}] Failed to route cross-server message`, err);
    }
});
/**helper function to securely fetch offline messages sort them chronologically */
async function fetchAndDeliverOfflineMessages(userId,ws){
    try{
        const offlineInboxCollection = db.collection("offline_inbox");
        const offlineMessages = await offlineInboxCollection.find({ userId }).toArray();
        if(!offlineMessages || offlineMessages.length === 0){
            return;
        }
        offlineMessages.sort((a,b)=> new Date(a.timestamp) - new Date(b.timestamp));
        for(const msg of offlineMessages){
            if(ws.readyState === WebSocket.OPEN){
                ws.send(JSON.stringify({
                    messageId: msg.messageId,
                    senderId: msg.senderId,
                    targetId: msg.userId,
                    content: msg.content,
                    mediaUrl: msg.mediaUrl,
                    timestamp: msg.timestamp
                }));
            }
        }
        // 4. Delete messages from Astra DB so they don't get delivered again on the next login
        await offlineInboxCollection.deleteMany({ userId });
        console.log(`[${SERVER_ID}] Successfully cleared offline cache for User ${userId}.`);
    } catch (err) {
        console.error(`[${SERVER_ID}] Failed to fetch and deliver offline messages`, err);
    }
}


wss.on('connection', async (ws, req) => {
    const { query } = parse(req.url, true);
    const plainToken = query.token;

    if (!plainToken) {
        ws.send(JSON.stringify({ type: "AUTH_ERROR", error: "Missing token parameter" }));
        ws.close(4001, "Missing token parameter");
        return;
    }
    // 2. Run your authentication function to get a definite answer immediately
    const authResult = await authenticateToken(plainToken);

    if (authResult.error) {
        // If an error happens (InvalidToken, InternalServerError), inform client and drop connection
        ws.send(JSON.stringify({ type: "AUTH_ERROR", error: authResult.error }));
        ws.close(4003, authResult.error);
        return;
    }

    // 3. Extract the trusted userId returned securely by your database hash check
    const userId = String(authResult.userId);


      
    

    // register users globally and locally
    localConnections.set(userId, ws);
    await pub.hset(REGISTRY_KEY, userId, SERVER_ID);
    console.log(`[${SERVER_ID}] User ${userId} joined.`);

    // Inform client that authentication succeeded
    ws.send(JSON.stringify({ type: "AUTH_SUCCESS", userId }));
    // ─── OFFLINE INBOX LOOP RUNS HERE ONCE USER OBJECT ID IS SECURELY BOUND ───
    await fetchAndDeliverOfflineMessages(userId, ws);

    ws.on('message', async (messageData) => {
        try {
            const parsedData = JSON.parse(messageData);

            const { targetId, content, mediaUrl } = parsedData;
            const senderId = userId;
            const messageId = parsedData.messageId || crypto.randomUUID();

            if (!targetId) return;

            // Standardize the payload structure to match our database schemas
            const structuredPayload = {
                messageId,
                senderId,
                targetId,
                content: content || null,     // Allows media-only uploads
                mediaUrl: mediaUrl || null,   // Optional asset link from Cloudinary
                timestamp: new Date().toISOString()
            };

            // this is what we send to the target (or publish via Redis)
            const serializedPayload = JSON.stringify(structuredPayload);

            // Route 1: Target is connected to this server
            if (localConnections.has(targetId)) {
                const localTargetWs = localConnections.get(targetId);
                if (localTargetWs.readyState === WebSocket.OPEN) {
                    localTargetWs.send(serializedPayload);
                }
                //notify user that msg is received
                // Notify sender that message was delivered directly on this instance
                ws.send(JSON.stringify({
                    status: "msg_ack",
                    messageId,
                    targetId,
                    deliveryMode: "direct"
                }));
                return;
                
            }

            // Route 2: Intelligent Global Routing
            const targetServerId = await pub.hget(REGISTRY_KEY, targetId);

            if (targetServerId) {
                const targetChannel = `channel_${targetServerId}`;
                await pub.publish(targetChannel, serializedPayload);
                //notify user msg is sent and received
                // Notify sender that message was routed over Redis pub/sub
                ws.send(JSON.stringify({
                    status: "msg_ack",
                    messageId,
                    targetId,
                    deliveryMode: "routed_cross_server"
                }));
            } else {
                console.log(`User ${targetId} is offline. Routing transaction to Astra DB...`);

                const offlineInboxCollection = db.collection("offline_inbox");

                // Drop a clean JSON document matching our unified layout into Astra DB
                await offlineInboxCollection.insertOne({
                    userId: targetId,           // Indexed primary lookup key for user reconnection
                    messageId,
                    senderId,
                    content: content || null,
                    mediaUrl: mediaUrl || null,
                    timestamp: new Date()       // Astra optimizes native Date objects automatically
                });

                // Notify the sender that the packet was safely deposited offline
                ws.send(JSON.stringify({
                    status: "cached_offline",
                    messageId,
                    targetId
                }));
            }
        } catch (err) {
            console.error("Invalid frame payload formatting received", err);
        }
    });

    // Cleanup logic on disconnect
    const disconnectHandler = async () => {
        // Only remove the local entry if it still points to THIS socket
        if (localConnections.get(userId) === ws) {
            localConnections.delete(userId);
        }

        try {
            // Safety verification: only delete from Redis if the user hasn't reconnected elsewhere
            const currentAssignedServer = await pub.hget(REGISTRY_KEY, userId);
            if (currentAssignedServer === SERVER_ID && !localConnections.has(userId)) {
                await pub.hdel(REGISTRY_KEY, userId);
            }
        } catch (err) {
            console.error(`[${SERVER_ID}] Failed to clean up registry for ${userId}`, err);
        }

        console.log(`[${SERVER_ID}] User ${userId} deregistered.`);
    };

    ws.on('close', disconnectHandler);
    ws.on('error', disconnectHandler);
});