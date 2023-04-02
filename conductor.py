import json,pika,datetime,argparse
from common import *
from peertube import *
from database import *

def send_message_work(dlBatch, metadata):

    # Set up RabbitMQ connection and channel
    mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'],get_az_secret('RMQ-CRED')['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='172.24.0.3',credentials=mq_cred))
    channel = connection.channel()

    # Declare the durable queue (create if not exists)
    queue_name = "work"
    channel.queue_declare(queue=queue_name, durable=True)

    # Send messages to the queue with message persistence and metadata
    message = json.dumps(dlBatch)
    channel.basic_publish(
        exchange="",
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
            headers=metadata  # Add metadata as headers
        )
    )

    # Close the connection
    connection.close()


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
        metadata = {
            "content_type": "application/json",
            "job_type": "dl_period",
            "period": period,
            "p_id": p_id,
            "version": "1.0"
        }
        send_message_work(batch,metadata)

    print(f'Sent {len(batches)} messages to worker queue.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run queue_dl_period from command line")
    parser.add_argument("-q", "--queue_dl_period", type=str, help="Period to queue the top posts (hour, day, week, month, year, all)")
    parser.add_argument("-b", "--batch_size", type=int, default=100, help="Batch size for messages in the queue (default: 100)")

    args = parser.parse_args()

    if args.queue_dl_period:
        queue_dl_period(args.queue_dl_period, args.batch_size)
    else:
        print("Please provide a period argument: --queue_dl_period")