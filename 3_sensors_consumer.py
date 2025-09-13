from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, prefix
import json

# Створення Kafka Consumer
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

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = f'{prefix}_building_sensors'

# Підписка на тему
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")

        data = None
        if message.value['sensor_type'] == 'temperature' and message.value['value'] > 40:
            data = {
                "topic_name": f'{prefix}_temperature_alerts',
                "message": "Invalid temperature alert!"
            }
        elif message.value['sensor_type'] == 'humidity' and (message.value['value'] > 80 or message.value['value'] < 20):
            data = {
                "topic_name": f'{prefix}_humidity_alerts',
                "message": "Invalid humidity alert!"
            }

        if data:
            print(f"Sending alert: {data['message']} to topic {data['topic_name']}")
            data['timestamp'] = message.value['timestamp']
            data['sensor_id'] = message.value['sensor_id']
            data['value'] = message.value['value']

            topic_name = data.pop('topic_name')
        
            producer.send(topic_name, value=data)
            producer.flush()

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer

