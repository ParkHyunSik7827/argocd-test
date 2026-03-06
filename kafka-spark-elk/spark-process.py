from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Kafka 및 Elasticsearch 설정
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "user_logs"
ES_NODES = "elasticsearch"
ES_PORT = "9200"
ES_INDEX = "processed_logs"

spark = SparkSession.builder \
    .appName("KafkaToES") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.17.3") \
    .getOrCreate()

# Kafka 데이터 읽기
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
  .option("subscribe", KAFKA_TOPIC) \
  .load()

# 데이터 스키마 정의 (JSON 가정)
schema = StructType().add("user_id", StringType()).add("action", StringType()).add("timestamp", StringType())

# 메시지 파싱
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Elasticsearch에 저장
query = parsed_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", ES_NODES) \
    .option("es.port", ES_PORT) \
    .option("es.resource", ES_INDEX) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()
