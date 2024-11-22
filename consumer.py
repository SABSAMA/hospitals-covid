from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_TOPIC = 'hospital_utilization_topic'
KAFKA_SERVER = 'localhost:9092'
HDFS_PATH = '/user/hdfs/integrated_data/'

spark = SparkSession.builder \
    .appName('HospitalUtilizationIntegration') \
    .getOrCreate()

# Initialisation du consommateur Kafka
consumer = KafkaConsumer(KAFKA_TOPIC,
                         bootstrap_servers=[KAFKA_SERVER],
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Fonction pour traiter les messages Kafka et les intégrer avec Spark
def process_kafka_stream():
    for message in consumer:
        data = message.value
        # Convertir les données en DataFrame Spark
        df = spark.read.json(spark.sparkContext.parallelize([data]))
        
        mortality_df = spark.read.csv('hdfs://localhost:9000/user/hdfs/in-hospital-mortality-trends-by-diagnosis-type.csv', header=True, inferSchema=True)
        enriched_df = df.join(mortality_df, on=['setting', 'date'], how='left')
        
        # Sauvegarder les résultats dans HDFS
        enriched_df.write.mode('append').parquet(HDFS_PATH)
        print("Data integrated and saved to HDFS")

# Lancer le traitement en streaming
process_kafka_stream()
