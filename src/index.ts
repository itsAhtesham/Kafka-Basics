import express from 'express';
import { Kafka } from 'kafkajs';
import { WebSocket } from 'ws';

const KAFKA_BROKER = 'localhost:9092';
const TOPIC = 'chat-topic';

const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: [KAFKA_BROKER]
});

const producer = kafka.producer();

async function initializeKafka() {
  await producer.connect();
  console.log('Kafka Producer is connected.');
}


function createChatServer(port: number) {
    const app = express();
    const server = app.listen(port, () => {
      console.log(`Chat server running on port ${port}`);
    });
  
    const wss = new WebSocket.Server({ server });
    const clients: Set<WebSocket> = new Set();
  
    const consumer = kafka.consumer({ groupId: `chat-group-${port}` });
  
    (async () => {
      await consumer.connect();
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      console.log(`Kafka Consumer connected on port ${port}`);
    
      consumer.run({
        eachMessage: async ({ message }) => {
          console.log(`Message from Kafka on port ${port}:`, message.value!.toString());
          for (const client of clients) {
            if (client.readyState === WebSocket.OPEN) {
              client.send(message.value!.toString());
            }
          }
        },
      });
    })();
  
    wss.on('connection', (ws) => {
      console.log('Client connected.');
      clients.add(ws);
  
      ws.on('message', async (message) => {
        console.log(`Received: ${message}`);
  
        try {
          await producer.send({
            topic: TOPIC,
            messages: [{ value: message.toString() }],
          });
          console.log('Message sent to Kafka:', message.toString());
        } catch (err) {
          console.error('Error sending message to Kafka:', err);
        }
      });
  
      ws.on('close', () => {
        console.log('Client disconnected.');
        clients.delete(ws);
      });
    });
}  


(async () => {
  await initializeKafka();

  createChatServer(3000);
  createChatServer(4000);
})();
