import pika
import datetime as dt
import time
import random

# Establish a connection with RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters('localhost')
)

# Create a channel
channel = connection.channel()

categories = ['ORDEM', 'ORCAMENTO']
priorities = ['ALTA', 'MEDIA', 'BAIXA']
# statuses = ['ABERTA', 'ENCERRADA']
department = 'SALES'

# Create a message 
for i in range (10_000):

    # Assemble message
    category = random.choice(categories)
    priority = random.choice(priorities)
    # status = random.choice(statuses)

    time_stamp = dt.datetime.strftime(dt.datetime.now(), format='%Y-%m-%d %H:%M:%S.%f')
    message = f'{time_stamp} {i:6} Mensagem criada por {department} com prioridade {priority} para informar sobre {category}.'


## REGRAS DE ENVIO


## 1. Todos orçamentos  são publicados no modo *direct*, rota *orcamentos*.

    if (category=='ORCAMENTO') :

        # Define exchange name and route
        exchange_name_ = 'exchange_direct'
        route_ = 'orcamentos'

        # Publish message
        channel.basic_publish(
            exchange=exchange_name_,
            routing_key=route_,
            body=message
        )

## 2. Todas ordens de compra  são publicados no modo *direct*, rota *ordens*.

    if category == 'ORDEM':
        # Define exchange name and route
        exchange_name_ = 'exchange_direct'
        route_ = 'ordens'

        # Publish message
        channel.basic_publish(
            exchange=exchange_name_,
            routing_key=route_,
            body=message
        )    
  
    print(f" [x] Sent {message}")
    time.sleep(random.randint(0,3))



# Close the connection
connection.close()