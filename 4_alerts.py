from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, prefix
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id='sensors'   # Ідентифікатор групи споживачів
)

topics_name = [f'{prefix}_temperature_alerts', f'{prefix}_humidity_alerts']

# Підписка на топіки
consumer.subscribe(topics_name)

print(f"Subscribed to topics: {topics_name[0]}, {topics_name[1]}")

try:
    for message in consumer:
        print(f"Received message: {message.value}, partition {message.partition}")
        print(f"Message: {message.value['message']}, Sensor ID: {message.value['sensor_id']}, Value: {message.value['value']}, Timestamp: {message.value['timestamp']}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
