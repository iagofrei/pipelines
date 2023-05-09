# Importando as bibliotecas necessárias
from kafka import KafkaProducer
from faker import Faker
import json
import time
import random
import uuid
import datetime as dt

# Instanciando o gerador de dados falsos
fake = Faker()


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
        # Gerar uma transação
        transaction = generate_transaction()
        print(transaction)

        # Adormecer por um período aleatório de tempo entre 0.5 e 2 segundos
        time.sleep(random.uniform(0.5, 2))

# Certificando-se de que o script é executado apenas quando é o script principal
if __name__:
    main()
