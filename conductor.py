import json,pika,datetime
from decimal import Decimal
from common import *
from peertube import *
from database import *

def send_message_work(dlBatch):

    # Set up RabbitMQ connection and channel
    mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'],get_az_secret('RMQ-CRED')['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=f"{get_az_secret('RMQ-CRED')['url']}",credentials=mq_cred))
    channel = connection.channel()

    # Declare the durable queue (create if not exists)
    queue_name = "work"
    channel.queue_declare(queue=queue_name, durable=True)

    # Send messages to the queue with message persistence
    message = json.dumps(dlBatch)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # Make message persistent
    )

    # Close the connection
    connection.close()


import json  # Make sure to import the json module at the beginning of your file

def queue_dl_period(period, batch_size=100):
    today = datetime.today().strftime('%m-%d-%Y')
    peertube_auth()
    p_title = f'Top of the {period} for all subs as of {today}'
    p_id = create_playlist(p_title, 2)

    dlList = get_dl_list_period(period)

    print(f'Adding {len(dlList)} posts from {period} for all subreddits to the worker queue.')

    # Split dlList into batches
    batches = [dlList[i:i + batch_size] for i in range(0, len(dlList), batch_size)]

    for batch in batches:
        json_batch = json.dumps(batch)
        first_part = f'{{"p_id": "{p_id}", "period": "{period}"}}'
        b = f'{first_part} | {json_batch}'
        send_message_work(b)

    print(f'Sent {len(batches)} messages to worker queue.')