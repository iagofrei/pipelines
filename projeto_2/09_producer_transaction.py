# Importando as bibliotecas necessárias
from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid
import datetime as dt
from config import KAFKA_BROKERS, TOPICS, CLIENTS, PARTITIONS

# Instanciando o gerador de dados falsos
fake = Faker()

# Configura os brokers
bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Cria o produtor
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: x.encode('utf-8'),
    key_serializer=lambda x: x.encode('utf-8'),
)

# Lista de tópicos e partições
topic_partitions = [
    {'topic': 'transactions', 'partitions': 1}
]

# Funções de callback
def on_send_success(record_metadata):
    delivery_time = dt.datetime.fromtimestamp(record_metadata.timestamp / 1e3)
    print(f'Mensagem enviada com sucesso em {delivery_time} para o tópico {record_metadata.topic} na partição {record_metadata.partition} com offset {record_metadata.offset}')
    print('\n')

def on_send_error(ex):
    print(f'Erro ao enviar mensagem: {ex}')

# Função para gerar uma transação fictícia
def generate_transaction():
    trans_time = dt.datetime.now()
    return {
        "customer_id": random.randint(1, 10),
        "timestamp": trans_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        "transaction_id": fake.uuid4(),
        "amount": round(random.uniform(1, 1000), 2),
        "currency": "USD",
        "card_brand": random.choice(["Visa", "MC", "Elo", "Amex"]),
    }

# Função principal para enviar transações para o tópico Kafka
def main():
    while True:

        # Escolhe o tópico
        topic = 'transactions'
        partition_ = 1
        key_ = f'key_test' #{topic}-partition-{partition_}'

        # Gerar uma transação
        transaction = generate_transaction()
        print(transaction)

        # Envia a mensagem para o Kafka
        future = producer.send(TOPICS["transactions"], key={"transaction_id": transaction["transaction_id"], "customer_id": transaction["customer_id"], "card_brand": transaction["card_brand"]}, value=transaction)

        # Espera a confirmação da entrega
        try:
            record_metadata = future.get(timeout=10)
            on_send_success(record_metadata)
        except Exception as ex:
            on_send_error(ex)

        # Adormecer por um período aleatório de tempo entre 0.5 e 2 segundos
        time.sleep(random.uniform(0.5, 2))

# Certificando-se de que o script é executado apenas quando é o script principal
if __name__:
    main()

# Encerra o produtor
producer.flush()
producer.close()