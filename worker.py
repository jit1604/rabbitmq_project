import pika
from time import sleep

def callback(ch, method, properties, body):
    print(f"[x] Received: {body.decode()}")
    # simulate processing
    sleep(1)
    print("[✓] Done")

connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
channel = connection.channel()
channel.queue_declare(queue="message_queue")
channel.basic_consume(queue="message_queue", on_message_callback=callback, auto_ack=True)

print("[*] Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
