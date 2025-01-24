from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# create kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# define new topics
my_name = "kry"
topic_name_1 = f'{my_name}_athlete_event_results'
topic_name_2 = f'{my_name}_enriched_athlete_avg'

num_partitions = 2
replication_factor = 1

def create_topic(topic_name, num_partitions, replication_factor):
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    try:
        admin_client.create_topics(new_topics = [new_topic], validate_only=False)
        print(f'Topic "{topic_name}" created successfully.')
    except Exception as e:
        print(f'An exception occurred: {e}')

for topic in [topic_name_1, topic_name_2]:
    create_topic(topic, num_partitions, replication_factor)


# close connection with client
admin_client.close()