import pika
import sys
import os
import time
import random


def main():
    # Create a connection to the RabbitMQ server running on the local machine
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    queue_name_ = 'participacao_obrigatoria_mkt'

    # Define a callback function to handle incoming messages
    def callback(ch, method, properties, body):
        
        print(f" [x] Received {body}")
        print(f"{method.routing_key}")
        
        ch.basic_ack(delivery_tag = method.delivery_tag)

        time.sleep(random.randint(2,14))
        print('[X] Done.')

    # Set up a consumer to receive messages from the queue and pass them to the callback function
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=queue_name_,
        on_message_callback=callback,
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