from kafka import KafkaConsumer
from json import dumps



def my_kafka_consumer(): 
    consumer = KafkaConsumer('topic7', bootstrap_servers='kafka-release.kafka.svc.cluster.local:9092')
    for message in consumer:
        print(f"MyKafkaService2 -- --------------")
        process_message(message)


def process_message(message) -> None:
    try:
        a= message.value.decode('utf-8')
    except Exception as e:
        a = message.value
    print(f"message -- {message}")
    print(a)
    
my_kafka_consumer()