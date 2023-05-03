import pika

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

# Create a channel
channel = connection.channel()

# Define exchange name
exchange_name = 'exchange_direct'

# Declare exchange
channel.exchange_declare(
    exchange=exchange_name,
    exchange_type='direct'
)