import json,pika,time
from common import *
from threading import Timer

# Set up RabbitMQ connection and channel
mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'], get_az_secret('RMQ-CRED')['password'])
connection = pika.BlockingConnection(pika.ConnectionParameters(host=f"{get_az_secret('RMQ-CRED')['url']}", credentials=mq_cred))
channel = connection.channel()

# Declare the durable queue (create if not exists)
queue_name = "work"
channel.queue_declare(queue=queue_name, durable=True)

# Counter to keep track of the number of messages received
message_buffer = []
buffer_max_size = 5
buffer_timeout = 5  # seconds

def process_messages(messages):
    message_count = 0
    for message in messages:
        message_count += 1
        time.sleep(5)
        print(f"Processed: {message_count}")

def flush_buffer():
    global message_buffer
    if message_buffer:
        process_messages(message_buffer)
        message_buffer = []

def on_buffer_timeout():
    flush_buffer()

timer = Timer(buffer_timeout, on_buffer_timeout)

# Define a callback function to handle incoming messages
def on_message(channel, method, properties, body):
    global message_buffer, timer
    message = json.loads(body)
    print(f"Received: a message")
    channel.basic_ack(delivery_tag=method.delivery_tag)

    message_buffer.append(message)

    if not timer.is_alive():
        timer.cancel()
        timer = Timer(buffer_timeout, on_buffer_timeout)
        timer.start()

    if len(message_buffer) >= buffer_max_size:
        flush_buffer()
        timer.cancel()

# Start consuming messages from the queue
channel.basic_qos(prefetch_count=5)
channel.basic_consume(queue=queue_name, on_message_callback=on_message)

print("Waiting for messages. Press CTRL+C to exit.")
channel.start_consuming()