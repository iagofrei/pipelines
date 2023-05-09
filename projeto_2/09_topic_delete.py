from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"]
)

topics_to_delete = ['__consumer_offsets', 'transactions', 'fraud_detection', 'technical_validation', 'authorizations']


for topic in topics_to_delete:
    try:
        delete_result =  admin_client.delete_topics([topic])
        print(delete_result)
        print('\n')
    except Exception as ex:
        print(f'Erro: {ex}')
        print('\n')
