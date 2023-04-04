import asyncio
import json
import logging
from aio_pika import connect_robust, Message, DeliveryMode, ExchangeType

from common import *
from main import *

# Configure keep-alive settings
heartbeat_interval = 60
socket_timeout = 2 * heartbeat_interval


async def connect_rabbit(queue='work'):
    rmq_url = get_az_secret('RMQ-CRED')['url']
    username = get_az_secret('RMQ-CRED')['username']
    password = get_az_secret('RMQ-CRED')['password']

    connection_string = f"amqp://{username}:{password}@{rmq_url}?heartbeat={heartbeat_interval}"
    connection = await connect_robust(connection_string)
    
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    await channel.declare_queue(queue, durable=True)
    return connection, channel


async def process_dl_period(period, p_id, batch):
    try:
        await asyncio.to_thread(main_dl_period, period, p_id, batch)
        clear_tmp_folder()
        print(f"Completed processing batch")
    except Exception as e:
        print(f"Error processing batch {e}")


async def on_message(message):
    async with message.process():
        headers = message.headers
        if headers['job_type'] == 'dl_period':
            p_id = headers['p_id']
            period = headers['period']
            json_batch = json.loads(message.body)
            print(f"Received: a message")
            await process_dl_period(period, p_id, json_batch)
            print("Completed this batch, waiting for next message...")
            # Acknowledge the message after processing
            await message.ack()
        elif headers['job_type'] != 'dl_period':
            print("Unknown job type, skipping message processing")
            logging.info("Unknown job type, skipping message processing")
            await message.reject(requeue=True)


async def main():
    event = asyncio.Event()
    while True:
        try:
            # Connect to RabbitMQ and start consuming messages
            connection, channel = await connect_rabbit()
            queue = await channel.declare_queue('work', durable=True)
            await queue.consume(on_message)

            print("Waiting for messages. Press CTRL+C to exit.")
            await event.wait()  # Use the created Event instance here
        except (KeyboardInterrupt, Exception) as e:
            if isinstance(e, KeyboardInterrupt):
                print("Exiting gracefully...")
                break
            print(f"Error: {e}")
            print("Attempting to reconnect in 10 seconds...")
            await asyncio.sleep(10)
        finally:
            try:
                await connection.close()
            except Exception as e:
                print(f"Error closing connection: {e}")


if __name__ == "__main__":
    asyncio.run(main())