import pika
import json
from langchain_ollama import OllamaLLM
from langchain.callbacks.base import BaseCallbackHandler

class RabbitMQStreamer(BaseCallbackHandler):
    def __init__(self, channel, queue):
        self.channel = channel
        self.queue = queue

    def on_llm_new_token(self, token: str, **kwargs):
        self.channel.basic_publish(exchange="", routing_key=self.queue, body=token)

def handle_message(ch, method, properties, body):
    message = json.loads(body.decode())
    message_id = message["id"]
    prompt = message["text"]

    response_queue = f"response_{message_id}"
    ch.queue_declare(queue=response_queue)

    callback = RabbitMQStreamer(ch, response_queue)
    llm = OllamaLLM(
        model="mistral",
        base_url="http://ollama:11434",
        callbacks=[callback]
    )
    llm(prompt)
    ch.basic_publish(exchange="", routing_key=response_queue, body="[END]")

connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
channel = connection.channel()
channel.queue_declare(queue="chat_queue")

print("[*] Worker waiting for chat messages...")
channel.basic_consume(queue="chat_queue", on_message_callback=handle_message, auto_ack=True)
channel.start_consuming()
