import pika

# Create a connection to the RabbitMQ server running on the local machine
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

queue_name_1 = 'informacoes_criticas'
queue_name_2a = 'participacao_obrigatoria_it'
queue_name_2b = 'participacao_obrigatoria_sales'
queue_name_2c = 'participacao_obrigatoria_mkt'
queue_name_2d = 'participacao_obrigatoria_rh'
queue_name_3 = 'compras'
queue_name_4 = 'logs'


# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_1
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_2a
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_2b
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_2c
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_2d
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_3
)

# Declare a queue to consume messages from
result = channel.queue_delete(
    queue=queue_name_4
)

# Declare a queue to consume messages from
result = channel.exchange_delete('exchange_topic')
result = channel.exchange_delete('exchange_direct')
result = channel.exchange_delete('exchange_fanout')


connection.close()