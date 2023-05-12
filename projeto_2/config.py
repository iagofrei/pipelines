# Define os endereços dos servidores Kafka (brokers)
KAFKA_BROKERS = ["localhost:9092", "localhost:9093", "localhost:9094"]

# Define os tópicos que serão utilizados nas mensagens Kafka
TOPICS = {
    "transactions": "transactions",
    "fraud_detection": "fraud_detection",
    "technical_validation": "technical_validation",
    "authorizations": "authorizations",
}

# Define os grupos de consumidores que serão utilizados
GROUPS = {
    'fraud_group': 'fraud_group',
    'validation_group': 'validation_group',
    'enrichment': 'enrichment',
    'results_group': 'results_group',
}

# Define o número de clientes que serão usados
CLIENTS = 10

# Define o número de partições a serem usadas nos tópicos Kafka
PARTITIONS = 1  

# Define o fator de replicação para os tópicos Kafka
REPLICATION_FACTOR = 3  
