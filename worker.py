import pika
from main import grab_dat
from database import process_subreddit_update

# Define callback function to process received messages
def callback(ch, method, properties, body):
    message = body.decode()
    print(f"Received message: {message}")

    if message == "":
        grab_dat()
    elif message == "process_subreddit_update":
        process_subreddit_update()
    else:
        print(f"Unknown message: {message}")

# Connection and channel creation
connection_params = pika.ConnectionParameters(host='172.19.0.2')
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare the queue (if it doesn't exist, it will be created)
queue_name = 'work'
channel.queue_declare(queue=queue_name)

# Bind the callback function to the queue and start consuming messages
channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

print('Waiting for messages...')
try:
    channel.start_consuming()
except KeyboardInterrupt:
    print('Interrupted, stopping...')
    channel.stop_consuming()
    connection.close()