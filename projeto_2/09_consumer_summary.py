# Importando as bibliotecas necessárias
from kafka import KafkaConsumer, TopicPartition
import json
from config import KAFKA_BROKERS, TOPICS, CLIENTS, PARTITIONS, GROUPS

# Configurando o consumidor Kafka
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKERS,
    value_deserializer=lambda v: json.loads(v),  # Deserializando os valores recebidos
    group_id=GROUPS['results_group']  # Definindo o grupo do consumidor
)

# Configurando a partição do tópico
tp = TopicPartition(TOPICS["authorizations"], 0)
consumer.assign([tp])

# Obtendo o offset atual e o offset final
current_offset = 0 # consumer.position(tp)
end_offset = consumer.end_offsets([tp])[tp]

# Buscando o offset atual
consumer.seek(tp, current_offset)

print("authorization", current_offset, end_offset)

# Definindo a função para contar as transações
def count_transactions():
    # Dicionários para armazenar as contagens de transações aprovadas e recusadas
    approved_counts = {}
    declined_counts = {}

    # Iterando sobre as mensagens no consumidor
    for message in consumer:
        # Extraindo o resultado da validação da mensagem
        validation_result = message.value
        # Extraindo a marca do cartão da mensagem
        brand = validation_result['card_brand']

        # Se a transação for válida, incrementamos a contagem de aprovações
        if validation_result['valid']:
            if brand in approved_counts:
                approved_counts[brand] += 1
            else:
                approved_counts[brand] = 1
        # Se a transação não for válida, incrementamos a contagem de recusos
        else:
            if brand in declined_counts:
                declined_counts[brand] += 1
            else:
                declined_counts[brand] = 1

        # Imprimindo a contagem de transações aprovadas e recusadas
        print(f"Approved transactions: {approved_counts}")
        print(f"Declined transactions: {declined_counts}")
        print('\n')

# Certificando-se de que a função count_transactions() é chamada apenas se este script for executado diretamente
if __name__ == "__main__":
    count_transactions()
