from kafka import KafkaProducer
import csv
import json
import time

# Paramètres Kafka
KAFKA_TOPIC = 'hospital_utilization_topic'
KAFKA_SERVER = 'localhost:9092'

data_file = './hospital-utilization-trends.csv'

producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def send_data_to_kafka():
    with open(data_file, 'r') as file:
        csv_reader = csv.DictReader(file)
        batch = []
        
        # Lire les lignes et envoyer en lots de 100
        for line in csv_reader:
            data = process_line(line)
            batch.append(data)
            
            # Si le batch atteint 100 messages, envoyer au topic Kafka
            if len(batch) == 100:
                producer.send(KAFKA_TOPIC, value=batch)
                print(f"Sent batch of 100 records to Kafka")
                batch = []
                time.sleep(10)
                
        # Envoyer le dernier batch restant
        if batch:
            producer.send(KAFKA_TOPIC, value=batch)
            print(f"Sent last batch of {len(batch)} records to Kafka")

# Traitement d'une ligne du fichier
def process_line(line):
    return {
        'setting': line['Setting'],
        'system': line['System'],
        'facility_name': line['Facility Name'],
        'date': line['Date'],
        'count': int(line['Count'])
    }

# Lancer le processus d'envoi
send_data_to_kafka()
