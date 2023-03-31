import json, pika
from common import *
from main import *

# Configure keep-alive settings
heartbeat_interval = 60
socket_timeout = 2 * heartbeat_interval

def connect_rabbit(queue='work'):
    mq_cred = pika.PlainCredentials(get_az_secret('RMQ-CRED')['username'], get_az_secret('RMQ-CRED')['password'])
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=get_az_secret('RMQ-CRED')['url'],
        credentials=mq_cred,
        heartbeat=heartbeat_interval,
        socket_timeout=socket_timeout
    ))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    return connection, channel


def process_dl_period(period, p_id, batch):
    try:
        main_dl_period(period, p_id, batch)
        clear_tmp_folder()
        print(f"Completed processing batch")
    except Exception as e:
        print(f"Error processing batch {e}")


def on_message(channel, method, properties, body):
    headers = properties.headers
    if headers['job_type'] == 'dl_period':    
        p_id = headers['p_id']
        period = headers['period']
        json_batch = json.loads(body)
        print(f"Received: a message")
        process_dl_period(period, p_id, json_batch)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    elif headers['job_type'] != 'dl_period':
        print("Unknown job type, skipping message processing")
        logging.info("Unknown job type, skipping message processing")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    # Connect to RabbitMQ and start consuming messages
    connection, channel = connect_rabbit()
    channel.basic_qos(prefetch_count=5)
    channel.basic_consume(queue='work', on_message_callback=on_message)

    print("Waiting for messages. Press CTRL+C to exit.")

    try:
        channel.start_consuming()
    except (KeyboardInterrupt, Exception) as e:
        print(e)
        print("Exiting gracefully...")
    finally:
        channel.stop_consuming()
        connection.close()

if __name__ == "__main__":
    main()