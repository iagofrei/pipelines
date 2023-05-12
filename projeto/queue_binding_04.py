import pika

# Create a connection to the RabbitMQ server running on the local machine
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


queue_name_ = 'compras'


# Mensagens vindas de E1
exchange_name_ = 'exchange_fanout'

routing_key_ = ''
channel.queue_bind(
            exchange=exchange_name_,
            queue=queue_name_,
            routing_key=routing_key_
        )


# Mensagens vindas de E2
exchange_name_ = 'exchange_direct'

routing_key_ = 'orcamentos'
channel.queue_bind(
            exchange=exchange_name_,
            queue=queue_name_,
            routing_key=routing_key_
        )


queue_name_ = 'logs'
routing_key_ = 'ordens'

channel.queue_bind(
            exchange=exchange_name_,
            queue=queue_name_,
            routing_key=routing_key_
        )


# Mensagens vindas de E3
exchange_name_ = 'exchange_topic'

routing_key_ = '#'
channel.queue_bind(
            exchange=exchange_name_,
            queue=queue_name_,
            routing_key=routing_key_
        )



connection.close()