import json, pika, time, ast
from common import *
from main import *

# Set up RabbitMQ connection and channel
mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'], get_az_secret('RMQ-CRED')['password'])
connection = pika.BlockingConnection(pika.ConnectionParameters(host=get_az_secret('RMQ-CRED')['url'], credentials=mq_cred))
channel = connection.channel()

# Declare the durable queue (create if not exists)
queue_name = "work"
channel.queue_declare(queue=queue_name, durable=True)


def process_message(period,p_id,batch):
    main_dl_period(period,p_id,batch)
    clear_tmp_folder()
    print(f"Completed processing batch")


# Define a callback function to handle incoming messages
def on_message(channel, method, properties, body):
    print(body)
    body_str = body.decode('utf-8')  # Decode the bytes object to a string
    parts = body_str.split("|")
    before_pipe = ast.literal_eval(parts[0].strip())
    after_pipe = parts[1].strip()
    p_id = before_pipe['p_id']
    period = before_pipe['period']
    json_batch = json.loads(after_pipe)
    print(f"Received: a message")
    process_message(period, p_id, json_batch)
    channel.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming messages from the queue
channel.basic_qos(prefetch_count=5)
channel.basic_consume(queue=queue_name, on_message_callback=on_message)

print("Waiting for messages. Press CTRL+C to exit.")

try:
    channel.start_consuming()
except (KeyboardInterrupt, Exception) as e:
    print(e)
    print("Exiting gracefully...")
finally:
    channel.stop_consuming()
    connection.close()