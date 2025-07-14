from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from pymongo import MongoClient
import pika
import uuid
import json

app = FastAPI()

client = MongoClient("mongodb://mongo:27017")
db = client["chatdb"]
collection = db["messages"]

RABBITMQ_HOST = "rabbitmq"

class Message(BaseModel):
    text: str

# Publish message to worker
@app.post("/chat")
def chat(msg: Message):
    message_id = str(uuid.uuid4())
    collection.insert_one({"_id": message_id, "text": msg.text})

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue="chat_queue")
    channel.basic_publish(
        exchange="",
        routing_key="chat_queue",
        body=json.dumps({"id": message_id, "text": msg.text})
    )
    connection.close()

    return {"message_id": message_id}

# Stream back response via SSE
@app.get("/stream/{message_id}")
async def stream_response(message_id: str):
    def event_stream():
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=f"response_{message_id}")

        for method_frame, properties, body in channel.consume(f"response_{message_id}", auto_ack=True):
            chunk = body.decode()
            yield f"data: {chunk}\n\n"
            if chunk.strip() == "[END]":
                break
        channel.queue_delete(queue=f"response_{message_id}")
        connection.close()

    return StreamingResponse(event_stream(), media_type="text/event-stream")
