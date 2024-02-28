from kafka import KafkaProducer
from json import dumps

try:
    print('before connection')
    kafka_config = {
            'bootstrap_servers': 'kafka-release.kafka.svc.cluster.local:9092',
            }
    producer = KafkaProducer(**kafka_config, value_serializer = lambda x:dumps(x).encode('utf-8'))
    print('Post connection')
    a = {
            "code": "accepted_risk",
            "parameter_id": "01a399f8-7bf1-4ba6-8158-709b24dc0ad6",
            "device_id": "01a399f8-7bf1-4ba6-8158-709b24dc0ad6",
            "org_id": "01a399f8-7bf1-4ba6-8158-709b24dc0ad6",
            "sent_user_id": "01a399f8-7bf1-4ba6-8158-709b24dc0ad6",
            "notification_time":"2023-09-15 14:58:29"
        }
    print('success connection')
    print(a)
    producer.send('topic7',value=a)
    print('sent success')
except Exception as e:
    print(str(e))
