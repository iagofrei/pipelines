# Importando as bibliotecas necessárias
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
from config import KAFKA_BROKERS, TOPICS, CLIENTS, PARTITIONS, GROUPS

# Instanciando três consumidores Kafka para diferentes tópicos
consumer_validations = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKERS,
    value_deserializer=lambda v: json.loads(v),
    group_id=GROUPS['enrichment'],
)

consumer_fraud = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKERS,
    value_deserializer=lambda v: json.loads(v),
    group_id=GROUPS['enrichment']
)

consumer_transaction = KafkaConsumer(
    bootstrap_servers=KAFKA_BROKERS,
    value_deserializer=lambda v: json.loads(v),
    group_id=GROUPS['enrichment']
)

# Instanciando o produtor Kafka com configurações específicas
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Definindo tópicos e partições para as quais os consumidores devem se inscrever
tp_validations = TopicPartition(TOPICS["technical_validation"], 0)
tp_fraud = TopicPartition(TOPICS["fraud_detection"], 0)
tp_transaction = TopicPartition(TOPICS["transactions"], 0)

# Atribuindo as partições aos consumidores
consumer_validations.assign([tp_validations])
consumer_fraud.assign([tp_fraud])
consumer_transaction.assign([tp_transaction])

# Obtendo o deslocamento atual nas partições
current_offset_validations = consumer_validations.position(tp_validations)
current_offset_fraud = consumer_fraud.position(tp_fraud)
current_offset_transaction = consumer_transaction.position(tp_transaction)

# Obtendo o deslocamento final nas partições
end_offset_validations = consumer_validations.end_offsets([tp_validations])[tp_validations]
end_offset_fraud = consumer_fraud.end_offsets([tp_fraud])[tp_fraud]
end_offset_transaction = consumer_transaction.end_offsets([tp_transaction])[tp_transaction]

# Imprimindo os deslocamentos atuais e finais para cada tópico
print("transactions", current_offset_transaction, end_offset_transaction)
print("technical_validation", current_offset_validations, end_offset_validations)
print("fraud_detection", current_offset_fraud, end_offset_fraud)

# Determinando o deslocamento inicial mais antigo entre as três partições e buscando esse deslocamento em todas elas
seek_trans = min(current_offset_transaction, current_offset_validations, current_offset_fraud)
consumer_validations.seek(tp_validations, seek_trans)
consumer_fraud.seek(tp_fraud, seek_trans)
consumer_transaction.seek(tp_transaction, seek_trans)

print(seek_trans)

def join_data():
    # Lendo mensagens dos três consumidores Kafka e juntando as transações correspondentes
    for msg_trans, msg_valid, msg_fraud in zip(consumer_transaction, consumer_validations, consumer_fraud):
        transaction_trans = msg_trans.value
        transaction_valid = msg_valid.value
        transaction_fraud = msg_fraud.value

        # Confirmando as mensagens para que não sejam lidas novamente
        consumer_transaction.commit()
        consumer_validations.commit()
        consumer_fraud.commit()

        # Verificando se as três transações correspondem
        if transaction_trans['transaction_id'] == transaction_valid['transaction_id'] == transaction_fraud['transaction_id']:
            # Se todas as transações correspondem, juntamos elas
            transaction_joined = transaction_trans
            transaction_joined['valid'] = transaction_valid['valid']
            transaction_joined['fraud'] = transaction_fraud['fraud']

            # Enviando a transação unida para o tópico "authorizations"
            producer.send(TOPICS["authorizations"], value=transaction_joined)
    
            print(transaction_joined)
        else:
            # Se as transações não correspondem, imprimimos uma mensagem de erro
            print(f"{transaction_trans['transaction_id']}, {transaction_valid['transaction_id']} and {transaction_fraud['transaction_id']} don't match!")

# Certificando-se de que a função join_data() é chamada apenas se este script for executado diretamente
if __name__ == "__main__":
    join_data()

