import pika

# 1. Estabelecendo uma conexão com o servidor RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

channel = connection.channel()

queue_name_1 = 'informacoes_criticas'
queue_name_2a = 'participacao_obrigatoria_it'
queue_name_2b = 'participacao_obrigatoria_sales'
queue_name_2c = 'participacao_obrigatoria_mkt'
queue_name_2d = 'participacao_obrigatoria_rh'
queue_name_3 = 'compras'
queue_name_4 = 'logs'

# 2. Criando a fila
channel.queue_declare(queue=queue_name_1,durable=True)
channel.queue_declare(queue=queue_name_2a,durable=True)
channel.queue_declare(queue=queue_name_2b,durable=True)
channel.queue_declare(queue=queue_name_2c,durable=True)
channel.queue_declare(queue=queue_name_2d,durable=True)
channel.queue_declare(queue=queue_name_3,durable=True)
channel.queue_declare(queue=queue_name_4,durable=True)