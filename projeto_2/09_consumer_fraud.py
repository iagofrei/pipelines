# Importando as bibliotecas necessárias
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import random
import time
from config import KAFKA_BROKERS, TOPICS, CLIENTS, PARTITIONS, GROUPS

# Instanciando o consumidor Kafka com configurações específicas
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKERS,
    value_deserializer=lambda v: json.loads(v),
    key_deserializer=lambda v: json.loads(v),
    group_id=GROUPS['fraud_group']
)

# Instanciando o produtor Kafka com configurações específicas
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Definindo o tópico e a partição para a qual o consumidor deve se inscrever
tp = TopicPartition(TOPICS["transactions"], 0)

# Atribuindo a partição ao consumidor
consumer.assign([tp])

# Obtendo o deslocamento atual na partição
current_offset = consumer.position(tp)

# Obtendo o deslocamento final na partição
end_offset = consumer.end_offsets([tp])[tp]

# Definindo a posição do consumidor no deslocamento atual
consumer.seek(tp, current_offset)

print("fraud_detection", current_offset, end_offset)

def validate_transactions():
    # Lendo mensagens do consumidor Kafka
    for message in consumer:
        key = message.key
        # Confirmando a mensagem para que não seja lida novamente
        consumer.commit()

        # Aprova aleatoriamente 99% das transações e marca 1% como fraude
        validation_result = {'transaction_id': key['transaction_id'], 'fraud': random.random() < 0.01}

        # Enviando o resultado da detecção de fraude para o tópico "fraud_detection"
        producer.send(TOPICS["fraud_detection"], key=key, value=validation_result)

        print(validation_result)

        # Adormecer por um período aleatório de tempo entre 0.1 e 3 segundos
        time.sleep(random.uniform(0.1, 3))

# Certificando-se de que a função validate_transactions() é chamada apenas se este script for executado diretamente
if __name__ == "__main__":
    validate_transactions()
