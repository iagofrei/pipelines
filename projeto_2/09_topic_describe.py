from kafka.admin import KafkaAdminClient

# Cria uma instância do KafkaAdminClient
admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
)

# Define o nome do tópico a ser descrito
topic_names = admin_client.list_topics()
topic_names = ['transactions', 'fraud_detection', 'technical_validation', 'authorizations']

# Descreve o tópico
topic_description = admin_client.describe_topics(topic_names)


print(f'{"Tópico":<9} {"Partição"} {"Líder"} {"Réplicas":<15} {"In-Sync Replicas (isr)":<10}  {"offline_replicas"} ')

for topic in topic_description:
    for partition in topic["partitions"]:
    
        print(f'{topic["topic"]:<12}  {partition["partition"]:<5}  {partition["leader"]:<5}  {partition["replicas"] } {"  ":10} {partition["isr"] } {"  ":20} {partition["offline_replicas"] } ' )
          