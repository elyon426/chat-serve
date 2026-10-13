import WebSocket from "ws";
import crypto from 'crypto';
import { parse } from "url";
//import { Redis } from '@upstash/redis';
import Redis from 'ioredis';


const localConnections = new Map();
const SERVER_ID = `server_${crypto.randomBytes(4).toString('hex')}`; 
const PORT = process.env.PORT || 8080;

const wss = new WebSocket({port:PORT});
console.log(`Chat server [${SERVER_ID}] running on port ${PORT}`);
//redis configuration for pub and sub
const redisUrl = new Redis(process.env.REDIS_URL);
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
        const { targetId, senderId, message } = JSON.parse(incomingPayload);
        
        if (!targetId || !message) return;
        
        // If the targeted user lives on this specific server instance, deliver it
        const localTargetWs = localConnections.get(targetId);
        if (localTargetWs && localTargetWs.readyState === WebSocket.OPEN) {
            localTargetWs.send(JSON.stringify({ senderId, message }));
        }
    } catch (err) {
        console.error(`[${SERVER_ID}] Failed to route cross-server message`);
    }
});

wss.on('connection',async(ws,req) => {
    const {query} = parse(req.url,true);
    const userId = query.userId;

    if(!userId){
        ws.close(4000,"Missing userId query parameter");
        return;
    }
    //register users globally and locally
    localConnections.set(userId,ws);
    await pub.hset(REGISTRY_KEY, userId, SERVER_ID);
    console.log(`[${SERVER_ID}] User ${userId} joined.`);

    ws.on('message',async (messageData) => {
        try{
            const parsedData = JSON.parse(messageData);
            const targetId = parsedData.targetId;

            if(localConnections.has(targetId)){
                const localTargetws = localConnections.get(targetId);
                if(localTargetws.readyState === WebSocket.OPEN){
                    localTargetws.send(messageData);
                }
                return;
            }
            // Route 2: Intelligent Global Routing
                const targetServerId = await pub.hget(REGISTRY_KEY, targetId);
                if (targetServerId) {
                   const mailPayload = JSON.stringify({ targetId, senderId: userId, message });
                   const targetChannel = `channel_${targetServerId}`;
                   pub.publish(targetChannel, mailPayload);
                } else {
                  ws.send(JSON.stringify({ error: `User ${targetId} is offline.` }));
                }

            
        }catch(err){
            console.error("Invalid frame payload formatting received")
        }
    });
    // Cleanup logic on disconnect
    const disconnectHandler = async () => {
        localConnections.delete(userId);
        
        // Safety verification: Only delete from Redis if the user hasn't reconnected elsewhere
        const currentAssignedServer = await pub.hget(REGISTRY_KEY, userId);
        if (currentAssignedServer === SERVER_ID) {
            await pub.hdel(REGISTRY_KEY, userId);
        }
        
        console.log(`[${SERVER_ID}] User ${userId} deregistered.`);
    };
    ws.on('close', disconnectHandler);
    ws.on('error', disconnectHandler);
});