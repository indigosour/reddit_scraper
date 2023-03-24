import pika

# Define callback function to process received messages
def callback(ch, method, properties, body):
    print(f"Received message: {body}")

# Connection and channel creation
connection_params = pika.ConnectionParameters(host='localhost')
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare the queue (if it doesn't exist, it will be created)
queue_name = 'toDownload'
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