from fastapi import FastAPI
from pydantic import BaseModel
import pika
from pymongo import MongoClient

app = FastAPI()

# MongoDB connection
client = MongoClient("mongodb://mongo:27017")
db = client["testdb"]
collection = db["messages"]

# RabbitMQ connection
def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    return connection.channel()

class Message(BaseModel):
    text: str

@app.post("/submit")
def submit_message(msg: Message):
    # Store in MongoDB
    collection.insert_one({"text": msg.text})
    
    # Publish to RabbitMQ
    channel = get_rabbitmq_channel()
    channel.queue_declare(queue="message_queue")
    channel.basic_publish(exchange="", routing_key="message_queue", body=msg.text)
    channel.close()
    
    return {"message": "Message received and queued."}
