from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers=['localhost:9092'],
)

topic_metadata = admin_client.list_topics()
print(topic_metadata)