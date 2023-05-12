import pika
import sys
import os
import time
import random


def main():
    # Create a connection to the RabbitMQ server running on the local machine
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Declare a queue to consume messages from
    result = channel.queue_declare(
        queue='', 
        exclusive=True
    )

    # Define queue bind para E1
    exchange_name = 'exchange_topic'
    queue_name = result.method.queue


    channel.queue_bind(
        exchange=exchange_name,
        queue=queue_name,
        routing_key='#.alta.#'
    )

    # Define queue bind para E2
    exchange_name = 'exchange_direct'
    queue_name = result.method.queue


    channel.queue_bind(
        exchange=exchange_name,
        queue=queue_name,
        routing_key='orcamentos'
    )

    channel.queue_bind(
        exchange=exchange_name,
        queue=queue_name,
        routing_key='ordens'
    )

    # Define queue bind para E3
    exchange_name = 'exchange_topic'
    queue_name = result.method.queue


    channel.queue_bind(
        exchange=exchange_name,
        queue=queue_name,
        routing_key='rh.movimentacao.*'
    )

    # Define a callback function to handle incoming messages
    def callback(ch, method, properties, body):

        print(f" [x] Received {body}")
        print(f"{method.routing_key}")

        time.sleep(random.randint(1,9))
        print('[X] Done.')


    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )

    # Start consuming messages from the queue
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')

        # Attempt to exit gracefully
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)