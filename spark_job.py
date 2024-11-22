from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum


spark = SparkSession.builder \
    .appName('HospitalUtilizationMetrics') \
    .getOrCreate()


hospital_df = spark.read.csv('hdfs://localhost:9000/user/hdfs/hospital-utilization-trends.csv', header=True, inferSchema=True)
mortality_df = spark.read.csv('hdfs://localhost:9000/user/hdfs/in-hospital-mortality-trends-by-diagnosis-type.csv', header=True, inferSchema=True)
health_category_df = spark.read.csv('hdfs://localhost:9000/user/hdfs/in-hospital-mortality-trends-by-health-category.csv', header=True, inferSchema=True)

enriched_df = hospital_df.join(mortality_df, on=['setting', 'date'], how='left')
final_df = enriched_df.join(health_category_df, on=['setting', 'date'], how='left')

metrics_df = final_df.groupBy('setting', 'date') \
    .agg(
        sum('count').alias('total_cases'),
        avg('Count').alias('average_mortality_rate')
    )

# Sauvegarder les résultats dans HDFS
metrics_df.write.mode('overwrite').parquet('hdfs://localhost:9000/user/hdfs/metrics_data/')

# Afficher les résultats
metrics_df.show()
